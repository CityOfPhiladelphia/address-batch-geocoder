# Publishing a Release

This code is intended to be called using an executable file generated from `powershell/geocoder_for_exe.ps1`. If changes are made to this file, we need to make a new release.

To create a release:

1. Update the exe_version variable in the ps1 file: `$exeVersion = "v1.1.0`
2. If necessary the min_exe_version.txt in the `powershell` folder. This is the minimum version a user should be allowed to run without being forced to redownload the exe.
3. Make sure the commit is tagged with a version number like `v1.0.0`:

```
# When you're ready to create a release:
git tag v1.0.0
git push origin v1.0.0
```

Publishing a new release will trigger the `build-and-publish` workflow, which calls the following command to create an executable file from the powershell script: `Invoke-ps2exe -inputFile $scriptFile -outputFile $outputExe -noConsole:$false`