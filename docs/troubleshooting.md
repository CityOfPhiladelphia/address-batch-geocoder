---
description: How to install the Address Batch Geocoder
icon: lucide/bug
---

# Bypassing the Windows Warning
If Windows shows a security warning when you first run the .exe, you'll need to bypass it (this is expected, a fix is in the works)​.

# Troubleshooting the geocoder.exe install

If you run into problems installing the geocoder, reach out to CityGeo via email, or open up a ticket with IT help.

Running `geocoder.exe` will create a `logs/` subdirectory in the same location as `geocoder.exe`. When resolving issues with installation or running the geocoder, you may be asked to provide CityGeo with the contents of this logfile. The logfile should be saved as `logs/geocoder_exe.log`.


## Common Issues

### Error: \[WinError 32\]
!!! failure "Error: \[WinError 32\] The process cannot access the file because it is being used by another process."

If you encounter this error, there is a good chance that you have the enriched file open in another application, such as excel. Close that file, and then attempt to geocode again.
