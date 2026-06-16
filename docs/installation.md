---
description: How to install the Address Batch Geocoder
icon: lucide/arrow-down-to-line
---

# How to Install the Geocoder


## :lucide-arrow-down-to-line: Download
First, you will need to download and install the geocoder. The Address Batch Geocoder can be downloaded here:

[:lucide-arrow-down-to-line: Download the latest release from GitHub](https://github.com/CityOfPhiladelphia/address-batch-geocoder/releases/latest/download/geocoder.zip){ .md-button .md-button--primary }
[:lucide-notepad-text: See the latest release notes](https://github.com/CityOfPhiladelphia/address-batch-geocoder/releases/latest){ .md-button }

!!! tip "Tip: Blocked file"
    You may get a dangerous file blocked warning from Chrome. The file is safe. You may override this block and proceed with downloading.

## :lucide-folder: Save to a Folder
Extract the zip folder into a folder where you can easily find it. When opening the zipped file, you may be prompted to either `extract` or `run`. Hit `extract`, not `run`, as the script will need to exist in an uncompressed directory in order to create the subfolders needed to work.

The zip folder contains two files: `geocoder.exe` and `release.txt`. If you delete or rename these files, you will need to download them again or rename them back. Deleting `release.txt` will stop the program from being able to inform you if there is a new version of the `.exe` file that you need to download.

!!! warning "Warning: Do not install the geocoder to OneDrive."
    One Drive saves temporary files and locks access to files in a way that corrupt the Geocoder installation. If you need to geocode files in OneDrive, It's better to install the Geocoder to your local desktop, and then give it the filepath to the files in OneDrive that you want to geocode.

## :lucide-play: Running the program for the first time
Double-clicking `geocoder.exe` will launch the program. You may see a popup that says "Windows protected your PC." This file is safe, so bypass this protection by clicking `More info,` and then selecting `Run anyway`.

As a first-time installation, the script will download Python and Git if not present, then download the geocoder from GitHub and install the proper dependencies. The geocoder will be downloaded to a folder called address-geocoder-main. If there are problems with your install, you may try deleting this folder and running `geocoder.exe` again.

!!! note "Note: Python 3.11"
    The installer will attempt to install Python 3.11 on your machine if you do not have Python 3.11 installed on your machine.

The script will then attempt to download the address file. The address file is the list of Philadelphia addresses that the tool makes the first-pass at matching against. This may take a few minutes. It will save the address file and a version file in a subfolder called `geocoder_address_data.` Under most circumstances, you should not remove this folder or any of the files in it. Doing so will cause the script to redownload the address file.

!!! success "You're ready to go."
    After the installation runs successfully, close the window and click `geocoder.exe` again to launch the program.