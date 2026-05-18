# Updating the Powershell Script

If you make changes to the powershell script, you may want to test how it works with the executable file. You will need to install and run Invoke-PS2EXE. Please use the following command to convert the powershell script to an exe. Do not omit the `-noConsole:$false` option, as we need that for the program to run in the console properly.

`Invoke-ps2exe -inputFile $scriptFile -outputFile $outputExe -noConsole:$false`

Do not commit the executable to the repo. Releasing the executable is handled via a github action, as described in section 4.3.