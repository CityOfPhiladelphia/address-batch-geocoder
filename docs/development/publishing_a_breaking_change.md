# Publishing a Breaking Change

If your updates will cause previous installs of the geocoder to fail to work, (For example, a major change to how the powershell script interfaces with the backend), you will need to create a release as mentioned above and update `powershell/min_exe_version.txt` to match the version in the change. This will prevent the user from being able to run the executable if their version is older than the breaking change.

To publish a breaking change:

1. Update `powershell/min_exe_version.txt` to the latest semver number. The format is `v2.0.0`. In the future, we plan to remove the 'v' from the string.
2. Update `powershell/geocoder_for_exe.ps1` so that `$exeVersion` variable matches the latest semver number
3. Please flag in any PR that you have created a breaking change, and have a second person review.
4. Once merged, create a release and push to main with the latest semver number

The check works as follows:

1. The exe should pull the most recent code on `main`, which will pull the latest copy of `min_exe_version.txt`.
2. The exe will check the executable version in the file against `powershell/min_exe_version.txt`. **You must update the $exeVersion variable in the powershell file for this to work**. This check works by stripping the "v" from the semver string, and then using powershell's `[System.Version]` to compare versions.
3. If the executable version is too low, the script will give the user an error and exit.