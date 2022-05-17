import argparse
import json
import logging
import os
import subprocess
import sys
import time
from itertools import repeat
from multiprocessing import Pool

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

CHECKPOINT0_FILENAME = "checkpoint0.json"
CHECKPOINT1_FILENAME = "checkpoint1.txt"
CHECKPOINT2_FILENAME = "checkpoint2.txt"
TARGET_CRS = "EPSG3035"  # European EPSG code.

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)
file_handler = logging.FileHandler('upload_vrt_to_artifactory.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stdout_handler)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser()
parser.add_argument("-u", "--username", help="User name", type=str, required=True)
parser.add_argument("-p", "--password", help="Password", type=str, required=True)
parser.add_argument("-o", "--output", help="Output vrt file name.", type=str, default="eu_croptype.vrt")
parser.add_argument("-a", "--adir", help="Directory on artifactory to convert to vrt.", type=str, default="eu_croptype")
parser.add_argument("-oz", "--overwritezone", help="Overwrite any existing zone vrt files in this directory. "
                                                   "Including reprojected ones.",
                    type=str2bool, nargs='?', default=True)
parser.add_argument("-or", "--overwritereproject", help="Overwrite any existing reprojected zone vrt files in this "
                                                        "directory.", type=str2bool, nargs='?', default=True)
parser.add_argument("-c", "--clean", help="Remove any intermediate files created by this script.",
                    type=str2bool, nargs='?', default=True)
parser.add_argument("-l", "--overviewlevels", help="Levels too use for overviews.",
                    type=int, nargs="+", default=[32, 64, 128])


def get_epsg(url):
    epsg = os.popen('gdalsrsinfo -o epsg ' + url).read().replace(" ", "").replace("\n", "")
    return epsg, url


def get_tile_urls(artifactory_directory):
    parsed_html = BeautifulSoup(os.popen('curl -s ' + artifactory_directory).read(), features="lxml")
    urls = []
    for link in parsed_html.findAll('a'):
        href = link.get('href')
        if href.endswith('.tif'):
            urls.append("/vsicurl/" + artifactory_directory + href)
    return urls


def map_to_zones(tile_urls):
    logger.info("Extracting epsg codes using gdalsrsinfo (this can take several minutes).")
    start = time.time()
    with Pool(processes=None) as pool:
        epsgs = pool.map(get_epsg, tile_urls)
    logger.info("Extraction took " + str(time.time() - start) + " seconds.")
    zone_files = {}
    for epsg, url in epsgs:
        epsg = epsg.replace(":", "")
        if epsg not in zone_files:
            zone_files[epsg] = []
        zone_files[epsg].append(url)
    return zone_files


def _build_zone_vrt(items, overwrite):
    (zone_url, zone_files) = items
    zone_output_filename = zone_url + ".vrt"
    if os.path.exists(zone_output_filename):
        if not overwrite:
            logger.info("Skipping vrt creation for zone {zone}, already exists.".format(zone=zone_url))
            return zone_output_filename
        os.remove(zone_output_filename)
    logger.info("Creating VRT for zone {zone}.".format(zone=zone_url))
    zone_input_files = " ".join(zone_files)
    p_input = 'gdalbuildvrt ' + zone_output_filename + ' ' + zone_input_files
    subprocess.check_call(p_input.split())
    return zone_output_filename


def build_zone_vrts(zone_to_tile_urls, overwrite):
    logger.info("Found zones: {zones}".format(zones=zone_to_tile_urls.keys()))
    with Pool(processes=None) as pool:
        zone_vrts = pool.starmap(_build_zone_vrt, zip(zone_to_tile_urls.items(), repeat(overwrite)))
    return zone_vrts


def _reproject_zone_vrt(zone_url, target_crs, overwrite):
    input_file = '/vsicurl/' + zone_url
    output_file = input_file.split("/")[-1].replace(".vrt", "_" + target_crs + ".vrt")
    if os.path.exists(output_file):
        if not overwrite:
            logger.info("Skipping reprojection of {input_file}, already exists on local file system.".format(input_file=input_file))
            return output_file
        os.remove(output_file)
    p_input = 'gdalwarp -of VRT {input} {output} -t_srs "{epsg}"'\
        .format(input=input_file, output=output_file, epsg=target_crs.replace("EPSG", "EPSG:"))
    os.system(p_input)
    return output_file


def reproject_zone_vrts(zone_vrt_urls, target_crs, overwrite):
    logger.info("Reprojecting all zone VRTs to srs: {epsg}".format(epsg=target_crs))
    with Pool(processes=None) as pool:
        zone_vrt_reprojected_filenames = pool.starmap(_reproject_zone_vrt, zip(zone_vrt_urls, repeat(target_crs), repeat(overwrite)))
    return zone_vrt_reprojected_filenames


def combine_zone_vrts(output_filename, zone_vrts_reprojected_urls):
    input_files = ['/vsicurl/' + vrt for vrt in zone_vrts_reprojected_urls]
    logger.info("Combining all zone VRTs into one: {output}".format(output=output_filename))
    p_input = 'gdalbuildvrt ' + output_filename + ' ' + " ".join(input_files)
    os.system(p_input)


def create_overview(output_filename, levels):
    logger.info("Creating overviews.")
    overview_levels_str = ' '.join([str(i) for i in levels])
    p_input = 'gdaladdo ' + '--config SPARSE_OK_OVERVIEW ON ' + '--config COMPRESS_OVERVIEW DEFLATE ' \
              + '--config GDAL_NUM_THREADS ALL_CPUS ' + output_filename + ' ' + overview_levels_str
    subprocess.check_call(p_input.split())


def upload_file_to_artifactory(artifactory_directory, output_filename, username, password):
    logger.info("Uploading {output_file} to artifactory.".format(output_file=output_filename))
    destination_url = artifactory_directory + output_filename
    p_input = "curl -u " + username + ":" + password + " -T " + output_filename + " " + destination_url
    subprocess.check_call(p_input.split())
    return destination_url


def clean_workdir(clean: bool, zone_to_tile_urls_filename, zone_vrts_filename, zone_vrt_reprojected_filenames_filename):
    if clean:
        logger.info("Cleaning up intermediate files.")
        with open(zone_vrts_filename, 'r') as f:
            zone_vrts = f.read().splitlines()
        with open(zone_vrt_reprojected_filenames_filename, 'r') as f:
            zone_vrt_reprojected_filenames = f.read().splitlines()
        for vrt in zone_vrts:
            os.remove(vrt)
        for vrt in zone_vrt_reprojected_filenames:
            os.remove(vrt)
        os.remove(zone_to_tile_urls_filename)
        os.remove(zone_vrts_filename)
        os.remove(zone_vrt_reprojected_filenames_filename)
    else:
        logger.info("Not cleaning up intermediate files as --clean was not set to True.")


def main():
    script_start_time = time.time()
    args = parser.parse_args()
    username = args.username
    password = args.password
    overwritezone = args.overwritezone
    overwriterepojected = args.overwritereproject
    overviewlevels = set(args.overviewlevels)
    clean = args.clean
    output_filename = args.output
    if not output_filename.endswith(".vrt"):
        raise ValueError("Output file must end with .vrt")
    artifactory_directory = 'https://artifactory.vgt.vito.be/auxdata-public/openeo/' + args.adir + '/'

    def checkpoint1_check():
        if os.path.exists(CHECKPOINT1_FILENAME):
            with open(CHECKPOINT1_FILENAME, "r") as f:
                zone_vrts = f.read().splitlines()
                for file_name in zone_vrts:
                    if not os.path.exists(file_name):
                        logger.info("{file} is not valid.".format(file=CHECKPOINT1_FILENAME))
                        return False
            return True
        return False

    def checkpoint2_check():
        if os.path.exists(CHECKPOINT2_FILENAME):
            with open(CHECKPOINT2_FILENAME, "r") as f:
                zone_vrt_reprojected_filenames = f.read().splitlines()
                for file_name in zone_vrt_reprojected_filenames:
                    if not os.path.exists(file_name):
                        logger.info("{file} is not valid.".format(file=CHECKPOINT2_FILENAME))
                        return False
            return True
        return False

    def checkpoint0_map_to_zones():
        logger.info("Checkpoint 0: Map to zones.")
        # 1. Retrieve geotiff links from artifactory.
        tile_urls = get_tile_urls(artifactory_directory)
        # 2. Map artifactory links to their zone.
        zone_to_tile_urls = map_to_zones(tile_urls)
        # End checkpoint.
        with open(CHECKPOINT0_FILENAME, "w") as f:
            json.dump(zone_to_tile_urls, f)
        return checkpoint1_build()

    def checkpoint1_build():
        # Start checkpoint.
        if not os.path.exists(CHECKPOINT0_FILENAME):
            logger.info("{file} is not valid.".format(file=CHECKPOINT0_FILENAME))
            return False
        with open(CHECKPOINT0_FILENAME, "r") as f:
            zone_to_tile_urls = json.load(f)
        logger.info("Checkpoint 1: Build VRTs.")
        # 3. Build one VRT per zone.
        logger.info("Building one VRT per zone.")
        start = time.time()
        zone_vrts = build_zone_vrts(zone_to_tile_urls, overwritezone)
        logger.info("Building VRTs took " + str(time.time() - start) + " seconds.")
        # End checkpoint.
        with open(CHECKPOINT1_FILENAME, 'w') as f:
            for zone_vrt in zone_vrts:
                f.write(zone_vrt + '\n')
        return checkpoint2_reproject()

    def checkpoint2_reproject():
        if not checkpoint1_check():
            return False
        # Start checkpoint.
        with open(CHECKPOINT1_FILENAME, 'r') as f:
            zone_vrts = f.read().splitlines()
        logger.info("Checkpoint 2: Reproject VRTs.")
        # Upload zone VRTs to artifactory.
        zone_vrt_urls = [upload_file_to_artifactory(artifactory_directory, zone_file, username, password)
                         for zone_file in zone_vrts]
        # 4. Reproject all VRTs to single EPSG code.
        zone_vrt_reprojected_filenames = reproject_zone_vrts(zone_vrt_urls, TARGET_CRS, overwriterepojected)
        # End checkpoint.
        with open(CHECKPOINT2_FILENAME, "w") as f:
            for zone_vrt_reprojected_filename in zone_vrt_reprojected_filenames:
                f.write(zone_vrt_reprojected_filename + '\n')
        return checkpoint3_combine()

    def checkpoint3_combine():
        if not checkpoint2_check():
            return False
        # Start checkpoint.
        with open(CHECKPOINT2_FILENAME, "r") as f:
            zone_vrt_reprojected_filenames = f.read().splitlines()
        logger.info("Checkpoint 3: Combine VRTs.")
        # Upload reprojected zone VRTs to artifactory.
        zone_vrts_reprojected_urls = [upload_file_to_artifactory(artifactory_directory, zone_file, username, password)
                                      for zone_file in zone_vrt_reprojected_filenames]

        # 5. Combine all zone VRTs into one large VRT and create overviews.
        combine_zone_vrts(output_filename, zone_vrts_reprojected_urls)
        create_overview(output_filename, overviewlevels)

        # Upload the final VRT with overviews to artifactory.
        upload_file_to_artifactory(artifactory_directory, output_filename, username, password)
        upload_file_to_artifactory(artifactory_directory, output_filename + ".ovr", username, password)

        clean_workdir(clean, CHECKPOINT0_FILENAME, CHECKPOINT1_FILENAME, CHECKPOINT2_FILENAME)
        return True

    checkpoints = [checkpoint0_map_to_zones, checkpoint1_build, checkpoint2_reproject, checkpoint3_combine]
    for checkpoint in reversed(checkpoints):
        if checkpoint():
            break
    logger.info("Script took " + str(time.time() - script_start_time) + " seconds.")


if __name__ == "__main__":
    main()
