import argparse
import os
import time
from multiprocessing import Pool

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup


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
parser.add_argument("-w", "--overwrite", help="Overwrite any existing vrt files in this directory.", type=str2bool, nargs='?', default=True)
parser.add_argument("-c", "--clean", help="Remove any intermediate files created by this script.", type=str2bool, nargs='?', default=True)


def get_epsg(url):
    epsg = os.popen('gdalsrsinfo -o epsg ' + url).read().replace(" ", "").replace("\n", "")
    return (epsg, url)

def main():
    script_start_time = time.time()
    args = parser.parse_args()
    username = args.username
    password = args.password
    overwrite = args.overwrite
    clean = args.clean
    output_filename = args.output
    target_crs = "EPSG3035"  # European EPSG code.
    if not output_filename.endswith(".vrt"):
        raise ValueError("Output file must end with .vrt")

    # Retrieve geotiff links from artifactory.
    artifactory_directory = 'https://artifactory.vgt.vito.be/auxdata-public/openeo/' + args.adir + '/'
    parsed_html = BeautifulSoup(os.popen('curl -s ' + artifactory_directory).read(), features="lxml")
    urls = []
    for link in parsed_html.findAll('a'):
        href = link.get('href')
        if href.endswith('.tif'):
            urls.append("/vsicurl/" + artifactory_directory + href)

    # Map artifactory links to their zone.
    print("Extracting epsg codes using gdalsrsinfo (this can take several minutes).")
    start = time.time()
    with Pool(processes=None) as pool:
        epsgs = pool.map(get_epsg, urls)
    print("Extraction took " + str(time.time() - start) + " seconds.")
    zone_files = {}
    for epsg, url in epsgs:
        epsg = epsg.replace(":", "")
        if epsg not in zone_files:
            zone_files[epsg] = []
        zone_files[epsg].append(url)

    # Build one VRT per zone.
    print("Found zones: {zones}".format(zones=zone_files.keys()))
    zone_vrts = []
    for zone_url in zone_files:
        zone_output_filename = zone_url + ".vrt"
        zone_vrts.append(zone_output_filename)
        if os.path.exists(zone_output_filename):
            if not overwrite:
                print("Skipping vrt creation for zone {zone}, already exists.".format(zone=zone_url))
                continue
            os.remove(zone_output_filename)
        print("Creating VRT for zone {zone}.".format(zone=zone_url))
        zone_input_files = " ".join(zone_files[zone_url])
        os.system('gdalbuildvrt ' + zone_output_filename + ' ' + zone_input_files)

    # Upload VRTs to artifactory.
    zone_vrt_urls = []
    for zone_file in zone_vrts:
        print("Uploading {output_file} to artifactory.".format(output_file=zone_file))
        destination_url = "{artifactory_directory}{output_file}".format(artifactory_directory=artifactory_directory, output_file=zone_file)
        # os.system("curl -u " + username + ":" + password + " -T " + zone_file + " " + destination_url)
        zone_vrt_urls.append(destination_url)

    # Reproject all VRTs to single EPSG code.
    print("Reprojecting all zone VRTs to srs: {epsg}".format(epsg=target_crs))
    zone_vrt_reprojected_filenames = []
    for zone_url in zone_vrt_urls:
        input_file = zone_url
        output_file = input_file.split("/")[-1].replace(".vrt", "_" + target_crs + ".vrt")
        zone_vrt_reprojected_filenames.append(output_file)
        if os.path.exists(output_file):
            if not overwrite:
                print("Skipping reprojection of {input_file}, already exists on local file system.".format(input_file=input_file))
                continue
            os.remove(output_file)
        os.system('gdalwarp -of VRT {input} {output} -t_srs "{epsg}"'
                  .format(input=input_file, output=output_file, epsg=target_crs.replace("EPSG", "EPSG:")))

    # # Upload reprojected zone VRTs to artifactory.
    zone_vrts_reprojected_urls = []
    for zone_file in zone_vrt_reprojected_filenames:
        print("Uploading {output_file} to artifactory.".format(output_file=zone_file))
        destination_url = "{artifactory_directory}{output_file}".format(artifactory_directory=artifactory_directory, output_file=zone_file)
        os.system("curl -u " + username + ":" + password + " -T " + zone_file + " " + destination_url)
        zone_vrts_reprojected_urls.append(destination_url)

    # Combine all zone VRTs into one large VRT.
    print("Combining all zone VRTs into one: {output}".format(output=output_filename))
    os.system('gdalbuildvrt ' + output_filename + ' ' + " ".join(zone_vrts_reprojected_urls))

    print("Creating overviews.")
    os.system('gdaladdo ' + output_filename + ' 8 16')

    # Upload the final VRT to artifactory.
    print("Uploading final VRT to artifactory.")
    destination_url = artifactory_directory + "/" + output_filename
    os.system("curl -u " + username + ":" + password + " -T " + output_filename + " " + destination_url)
    print("Uploading overviews to artifactory.")
    output_filename = output_filename.replace(".vrt", ".vrt.ovr")
    destination_url = artifactory_directory + "/" + output_filename
    os.system("curl -u " + username + ":" + password + " -T " + output_filename + " " + destination_url)

    if clean:
        print("Cleaning up intermediate files.")
        for vrt in zone_vrts:
            os.remove(vrt)
        for vrt in zone_vrt_reprojected_filenames:
            os.remove(vrt)
    else:
        print("Not cleaning up intermediate files as --clean was not set to True.")
    print("Script took " + str(time.time() - script_start_time) + " seconds.")


if __name__ == "__main__":
    main()
