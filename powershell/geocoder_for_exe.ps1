# Geocoder Powershell Script
#####

# Get the directory of the currently executing script
$ScriptDirectory = (Split-Path -Parent (Get-Process -Id $PID).Path)
$ScriptDirectory = (Resolve-Path -LiteralPath $ScriptDirectory).ProviderPath

# Paths needed for script
$installFolder      = Join-Path $ScriptDirectory 'address-geocoder-main'
$dataDirectory      = Join-Path $ScriptDirectory 'geocoder_address_data'
$powershellDirectory = Join-Path $installFolder 'powershell'
$logDirectory       = Join-Path $ScriptDirectory 'log'
$logFile            = Join-Path $logDirectory 'geocoder_exe.log' 
$versionFile        = Join-Path $ScriptDirectory 'release.txt'
$s3URL              = 'https://opendata-downloads.s3.amazonaws.com/address_service_area_summary_public.csv.gz'
$addressFileGZ      = Join-Path $dataDirectory   'address_service_area_summary.csv.gz'
$addressFileCSV     = Join-Path $dataDirectory   'address_service_area_summary.csv'
$addressFileParquet = Join-Path $dataDirectory   'address_service_area_summary.parquet'
$addressVersionFile = Join-Path $dataDirectory   'address_file.etag'
$venvPath           = Join-Path $installFolder   '.venv'
$venvPython         = Join-Path $venvPath 'Scripts\python.exe'
$configYml          = Join-Path $ScriptDirectory   'config.yml'
$configExample      = Join-Path $installFolder   'config_example.yml'
$toParquetPy        = Join-Path $installFolder   'csv_to_parquet.py'
$geocoderPy         = Join-Path $installFolder   'geocoder.py'

# GitHub Repo info
$repoURL = 'https://github.com/CityOfPhiladelphia/address-geocoder.git'
$owner = "CityOfPhiladelphia"
$repo = "address-batch-geocoder"
$branch = "origin/main"

# Exe version, used to check if we need to force user to update
$exeVersion = "2.2.0"


# Clear log file if exists, create log directory and log file if not exists
function initializeLogFile {

    if (-not (Test-Path -Path $logDirectory -PathType Container)) {

        New-Item -Path $logDirectory -ItemType Directory | Out-Null
        Write-Host "Created logging directory at $logDirectory." -ForegroundColor Green
    }

    if (-not (Test-Path -Path $logFile -PathType Leaf)) {

        New-Item -Path $logFile -ItemType File | Out-Null

    }

    # If the file already exists, overwrite it for this run
    else {
    
        "--- Geocoder.exe started at $(Get-Date) --- " *> $logFile

    }
}

function checkToolVersion {
    # Check if we have the version file (won't exist until repo is cloned)
    if (-not (Test-Path $versionFile)) {
        return
    }

    $localVersion = (Get-Content -Path $versionFile -Raw).Trim()

    # Get latest release from GitHub
    $apiUrl = "https://api.github.com/repos/$owner/$repo/releases/latest"
    
    try {
        $headers = @{
            'Accept' = 'application/vnd.github.v3+json'
            'User-Agent' = 'PowerShell-Geocoder'
        }
        
        $response = Invoke-RestMethod -Uri $apiUrl -Headers $headers -TimeoutSec 5 -ErrorAction Stop
        $remoteVersion = $response.tag_name
        
        # End process if version is out of date
        $minExeVersionFile = Join-Path $powershellDirectory 'min_exe_version.txt'
            if (Test-Path $minExeVersionFile) {
                $minExeVersion = (Get-Content $minExeVersionFile -Raw).Trim()
                if ([System.Version]($exeVersion.TrimStart('v')) -lt [System.Version]($minExeVersion.TrimStart('v'))) {
                    Write-Host "This version of the tool is no longer supported." -ForegroundColor Red
                    Write-Host "Please download the latest version from:" -ForegroundColor Yellow
                    Write-Host "https://github.com/$owner/$repo/releases/latest" -ForegroundColor Cyan
                    Read-Host "Press Enter to exit"
                    exit 1
                }
            }
        
        if ($localVersion -lt $remoteVersion) {
            $border = "=" * 70
            Write-Host ""
            Write-Host $border -ForegroundColor Yellow
            Write-Host "WARNING: A newer version of this tool is available!" -ForegroundColor Yellow
            Write-Host ""
            Write-Host "Your version:   $localVersion" -ForegroundColor White
            Write-Host "Latest version: $remoteVersion" -ForegroundColor Green
            Write-Host ""
            Write-Host "Please download the latest version from:" -ForegroundColor White
            Write-Host "https://github.com/$owner/$repo/releases/latest" -ForegroundColor Cyan
            Write-Host $border -ForegroundColor Yellow
            Write-Host ""
        } else {
            Write-Host "Tool version up to date ($localVersion)." -ForegroundColor Green
        }
    }
    catch {
        # Silently fail - don't want version check to break the tool
    }
}

function installGit {
    Write-Host "Checking for Git on this machine..."
    if (Get-Command git -ErrorAction SilentlyContinue) {
        Write-Host "Git is installed. Continuing."
    } else {
        Write-Host "Git not detected on machine. Installing git..."

        if (Get-Command winget.exe -ErrorAction SilentlyContinue) {
            $install_args = @(
                "install"
                "--id", "Git.Git"
                "--source", "winget"
                "--exact"
                "--silent"
                "--accept-package-agreements"
                "--accept-source-agreements"
            )

            $proc = Start-Process -FilePath "winget.exe" -ArgumentList $install_args -Wait -PassThru

            if ($proc.ExitCode -ne 0) {
                throw "Git installation via winget failed with exit code $($proc.ExitCode)."
            }
            
            # Refresh path after git install
            $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        } else {
            Write-Host "winget not found. Please install Git manually from https://git-scm.com/download/win" -ForegroundColor Yellow
            Read-Host "Press Enter to exit"
            exit 1
        }
    }
}

function installUv {
    Write-Host "Checking for uv on this machine..."
    if (Get-Command uv -ErrorAction SilentlyContinue) {
        Write-Host "uv is installed. Continuing."
    } else {
        Write-Host "uv not detected. Installing..."
        
        try {
            # Install uv using official installer
            powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
            
            # Refresh path
            $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
            
            # Verify installation
            if (Get-Command uv -ErrorAction SilentlyContinue) {
                Write-Host "uv installed successfully!" -ForegroundColor Green
            } else {
                throw "uv installation completed but command not found"
            }
        }
        catch {
            Write-Host "Failed to install uv: $_" -ForegroundColor Red
            Write-Host "Please install manually from https://docs.astral.sh/uv/" -ForegroundColor Yellow
            Read-Host "Press Enter to exit"
            exit 1
        }
    }
}

function createVenvAndConfig {
    Write-Host "Setting up virtual environment and packages..."

    Push-Location $installFolder

   "Installing packages with uv at $(Get-Date)" *>> $logFile
    & uv sync --link-mode=copy *>> $logFile
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Package installation failed." -ForegroundColor Red
        Read-Host "Press Enter to exit"
        exit 1
    }

    Pop-Location
        
    Write-Host "Package installation complete!" -ForegroundColor Green
    

    # Create config file if it doesn't exist
    if (-not (Test-Path -LiteralPath $configYml)) {
        if (Test-Path -LiteralPath $configExample) {
            Copy-Item -LiteralPath $configExample -Destination $configYml 
            Write-Host "Created config.yml from example. Please edit it with your settings."
        }
    }
}

function cloneOrUpdate {
    if (Test-Path $installFolder) {
        
        Write-Host "Repository exists. Checking for updates..."
        
        Push-Location $installFolder
        
        try {
            "Fetching updates from GitHub at $(Get-Date)" *>> $logFile
            git fetch origin *>> $logFile


            if ($LASTEXITCODE -ne 0) {
                Write-Host "Failed to fetch updates." -ForegroundColor Red
                Read-Host "Press Enter to exit"
                exit 1
            }
            
            $localCommit = git rev-parse HEAD
            $remoteCommit = git rev-parse $branch
            
            if ($localCommit -ne $remoteCommit) {
                Write-Host "Updates available. Pulling changes..."
                
                # Reset any local changes, they should always match remote
                git reset --hard $branch | ForEach-Object { Write-Host $_ }

                if ($LASTEXITCODE -ne 0) {
                    Write-Host "Failed to pull updates." -ForegroundColor Red
                    Read-Host "Press Enter to exit"
                    exit 1
                }
                
                Write-Host "Repository updated successfully!" -ForegroundColor Green
                $script:RepoWasUpdated = $true
            } else {
                Write-Host "Repository is up to date."
                $script:RepoWasUpdated = $false
            }
        }
        catch {
            Write-Host "Failed to update repository: $_" -ForegroundColor Red
            Read-Host "Press Enter to exit"
            exit 1
        }

        finally {
            Pop-Location
        }

    } else {
        Write-Host "Repository not found. Cloning..."
        
        try {
            "Cloning GitHub repository at $(Get-Date)" *>> $logFile
            git clone $repoURL $installFolder *>> $logFile

            if ($LASTEXITCODE -ne 0) {
                Write-Host "Failed to clone repository." -ForegroundColor Red
                Read-Host "Press Enter to exit"
                exit 1
            }
            
            Write-Host "Repository cloned successfully!" -ForegroundColor Green
            
            $script:RepoWasJustCloned = $true
        }
        catch {
            Write-Host "Failed to clone repository: $_" -ForegroundColor Red
            Write-Host "Stack trace: $($_.ScriptStackTrace)" -ForegroundColor Red
            Read-Host "Press Enter to exit"
            exit 1
        }
    }
}

function checkAddressFileVersion {
    
    $script:FileIsOutOfDate = $false

    if (Test-Path $addressVersionFile) {
        Write-Host "Checking for address file updates. This may take a few moments..."
        $localEtag = Get-Content -Path $addressVersionFile

        try {
            $response = Invoke-WebRequest -Uri $s3URL -Method Head -UseBasicParsing
            $remoteEtag = $response.Headers.Etag -replace '"', ''

            if ($localEtag -ne $remoteEtag) {
                Write-Host "Update available!" -ForegroundColor Yellow
                $script:FileIsOutOfDate = $true
            }

            else {
                Write-Host "Address file up to date." -ForegroundColor Green
            }
        }

        catch {
            Write-Host "Check failed (proceeding with local version)." -ForegroundColor Yellow
        }

    }

    else {

        $script:FileIsOutOfDate = $true
    }
}


function decompressFile {

    param (
        [string]$inFile,
        [string]$outFile
    )

    $inputStream = New-Object System.IO.FileStream $inFile, ([IO.FileMode]::Open)
    $gzipStream = New-Object System.IO.Compression.GZipStream $inputStream, ([IO.Compression.CompressionMode]::Decompress)
    $outputStream = New-Object System.IO.FileStream $outFile, ([IO.FileMode]::Create)

    $gzipStream.CopyTo($outputStream)

    $gzipStream.Close()
    $outputStream.Close()
    $inputStream.Close()
}

function downloadAddressFile {

    # Create data directory if it doesn't exist
    if (-Not (Test-Path $dataDirectory)) {
        New-Item -Path $dataDirectory -ItemType Directory | Out-Null
    }
    
    # Download address file if not present
    try {
        if ((-Not (Test-Path $addressFileGZ) -and -Not (Test-Path $addressFileCSV) -and -Not(Test-Path $addressFileParquet)) -or ($script:FileIsOutOfDate)) {

            if ($script:FileIsOutOfDate) {
                Write-Host "Address file is out of date. Downloading from S3. This may take a few minutes..." -ForegroundColor Yellow
            }

            else {
                Write-Host "Address file not found. Downloading from S3. This may take a few minutes..." -ForegroundColor Yellow
            }
            
            Invoke-WebRequest -Uri $s3URL -OutFile $addressFileGZ
            Write-Host "Download completed. Unzipping file..."
            decompressFile $addressFileGZ $addressFileCSV
            Remove-Item $addressFileGZ -Force

            # Get etag and save to file
            $response = Invoke-WebRequest -Uri $s3URL -Method Head -UseBasicParsing
            $remoteEtag = $response.Headers.Etag -replace '"', ''
            $remoteEtag | Out-File $addressVersionFile

        }
    }
    catch {
        Write-Host "Failed to download address file from S3." -ForegroundColor Red
        if (Test-Path $addressFileGZ) {
            Remove-Item $addressFileGZ -Force
        }

        if (Test-Path $addressFileCSV) {
            Remove-Item $addressFileCSV -Force
        }
        Read-Host "Press Enter to exit"
        exit 1
    }

    if (-Not (Test-Path $addressFileCSV) -and -Not (Test-Path $addressFileParquet)) {
        Write-Host "Unzipping file..."
        decompressFile $addressFileGZ $addressFileCSV
        Remove-Item $addressFileGZ -Force
    }
   
    # Convert address file to parquet if no parquet file present
    if (-Not (Test-Path $addressFileParquet) -or ($script:FileIsOutOfDate)) {   
        Write-Host "Converting address csv into a parquet file for speed and space optimization" -ForegroundColor Yellow
        
        Push-Location $installFolder
        & uv run $toParquetPy --input_path $addressFileCSV --output_path $addressFileParquet
        Pop-Location

        # Check if conversion failed
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Failed to convert address file into the proper format." -ForegroundColor Red
            
            # Remove partial parquet file if it exists
            if (Test-Path $addressFileParquet) {
                Remove-Item $addressFileParquet -Force
            }
            
            throw "CSV to Parquet conversion failed with exit code $LASTEXITCODE"
        }
    }

    # If all successful, remove the CSV file to save space
    if (Test-Path $addressFileCSV) {
        Remove-Item $addressFileCSV -Force
        
        Write-Host "`n========================================" -ForegroundColor Yellow
        Write-Host "ADDRESS FILE DOWNLOAD COMPLETE" -ForegroundColor Yellow
        Write-Host "========================================" -ForegroundColor Yellow
        Write-Host "Address file can be found at $addressFileParquet" -ForegroundColor Yellow
    }
}



$script:RepoWasJustCloned = $false
$script:RepoWasUpdated = $false

# Execute installation steps
initializeLogFile
installGit
installUv
cloneOrUpdate
checkToolVersion  # Check version after cloning/updating repo
createVenvAndConfig
checkAddressFileVersion
downloadAddressFile

# If repo was just cloned, user needs to configure before running
if ($script:RepoWasJustCloned) {
    Write-Host "`n========================================" -ForegroundColor Yellow
    Write-Host "FIRST TIME SETUP COMPLETE" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host "Please edit the config.yml file with your settings before running the geocoder."
    Write-Host "Config file location: $configYml"
    Write-Host "`nRun this script again after configuring to start the geocoder."
    Read-Host "`nPress Enter to exit"
    exit 0
}

# Always run the geocoder if not first-time setup
[string]$runOption = Read-Host -Prompt "Choose an option:`n[1] Run with the user-interface`n[2] Run with the .yml config`n[Any other key]: exit`n"

switch ($runOption) {
    '1' {
       try {
    
    # Surround with quotes to avoid issues with filepaths with spaces in the name
    $appPy = Join-Path $installFolder 'app.py'  # adjust to your actual entrypoint
    $appPy = '"' + $appPy + '"'

    Write-Host "Python: $venvPython"
    Write-Host "App: $appPy"
    Write-Host "Working dir: $ScriptDirectory"
    $process = Start-Process -FilePath $venvPython `
                            -ArgumentList "-m", "streamlit", "run", $appPy `
                            -WorkingDirectory $ScriptDirectory `
                            -NoNewWindow `
                            -Wait `
                            -PassThru

    if ($process.ExitCode -ne 0) {
        throw "Streamlit exited with code $($process.ExitCode)"
    }
}
catch {
    Write-Host "`n========== ERROR ==========" -ForegroundColor Red
    Write-Host "An error occurred while running the geocoder:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Yellow
    Write-Host "============================" -ForegroundColor Red
}
finally {
    Write-Host "`nProcess complete. Press any key to close..."
    [void][System.Console]::ReadKey($true)
} 
    }

    '2' {
        try {
    # Use Start-Process to allow interactive prompts
    # Surround with quotes to avoid issues with filepaths with spaces in the name
    $geocoderPy = '"' + $geocoderPy + '"'

    $process = Start-Process -FilePath $venvPython `
                             -ArgumentList $geocoderPy `
                             -WorkingDirectory $ScriptDirectory `
                             -NoNewWindow `
                             -Wait `
                             -PassThru
    
    if ($process.ExitCode -ne 0) {
        throw "Geocoder exited with code $($process.ExitCode)"
    }
}
catch {
    Write-Host "`n========== ERROR ==========" -ForegroundColor Red
    Write-Host "An error occurred while running the geocoder:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Yellow
    Write-Host "============================" -ForegroundColor Red
}
finally {
    Write-Host "`nProcess complete. Press any key to close..."
    [void][System.Console]::ReadKey($true)
}
    }
}