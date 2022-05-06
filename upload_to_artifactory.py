import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--username", help="User name", type=str, required=True)
parser.add_argument("-p", "--password", help="Password", type=str, required=True)
parser.add_argument("-d", "--directory", help="Directory to upload", type=str, required=True)
parser.add_argument("-a", "--adir", help="Artifactory directory name, defaults to eu_croptype", type=str, default="eu_croptype")


def main():
    args = parser.parse_args()
    username = args.username
    password = args.password
    directory = args.directory

    artifactory_directory = 'https://artifactory.vgt.vito.be/auxdata-public/openeo/' + args.adir + '/'
    for filename in os.listdir(directory):
        if filename.endswith(".tif"):
            source_path = directory + "/" + filename
            destination_url = artifactory_directory + filename
            # Support filenames with spaces.
            source_path = source_path.replace(' ', '\ ')
            destination_url = destination_url.replace(" ", "%20")
            print("Uploading " + source_path + " to " + destination_url)
            os.system("curl -u " + username + ":" + password + " -T " + source_path + " " + destination_url)


if __name__ == "__main__":
    main()
