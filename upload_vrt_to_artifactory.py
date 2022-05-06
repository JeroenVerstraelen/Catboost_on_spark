import argparse
import os
try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--username", help="User name", type=str, required=True)
parser.add_argument("-p", "--password", help="Password", type=str, required=True)
parser.add_argument("-o", "--output", help="Output vrt file name.", type=str, default="eu_croptype.vrt")
parser.add_argument("-d", "--adir", help="Directory on artifactory to convert to vrt.", type=str, default="eu_croptype")


def main():
    args = parser.parse_args()
    username = args.username
    password = args.password
    output_filename = args.output
    if not output_filename.endswith(".vrt"):
        raise ValueError("Output file must end with .vrt")

    artifactory_directory = 'https://artifactory.vgt.vito.be/auxdata-public/openeo/' + args.adir + '/'

    # Retrieve the links from artifactory.
    parsed_html = BeautifulSoup(os.popen('curl -s ' + artifactory_directory).read(), features="lxml")
    input_files = []
    for link in parsed_html.findAll('a'):
        href = link.get('href')
        if href.endswith('.tif'):
            input_files.append("/vsicurl/" + artifactory_directory + href)

    # Build the VRT.
    os.system('gdalbuildvrt ' + output_filename + ' ' + ' '.join(input_files))

    # Upload the VRT to artifactory.
    destination_url = artifactory_directory + "/" + output_filename
    os.system("curl -u " + username + ":" + password + " -T " + output_filename + " " + destination_url)


if __name__ == "__main__":
    main()
