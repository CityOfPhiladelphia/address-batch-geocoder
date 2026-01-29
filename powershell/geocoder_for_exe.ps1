# Get the directory of the currently executing script
$ScriptDirectory = (Split-Path -Parent (Get-Process -Id $PID).Path) 
$ScriptDirectory = (Resolve-Path -LiteralPath $ScriptDirectory).ProviderPath

# Paths needed for script
$installFolder      = Join-Path $ScriptDirectory 'address-geocoder-main'
$dataDirectory      = Join-Path $ScriptDirectory 'geocoder_address_data'
$powershellDirectory = Join-Path $installFolder 'powershell'
$versionFile        = Join-Path $powershellDirectory 'release.txt'
$s3URL              = 'https://opendata-downloads.s3.amazonaws.com/address_service_area_summary_public.csv.gz'
$addressFileGZ      = Join-Path $dataDirectory   'address_service_area_summary.csv.gz'
$addressFileCSV     = Join-Path $dataDirectory   'address_service_area_summary.csv'
$addressFileParquet = Join-Path $dataDirectory   'address_service_area_summary.parquet'
$addressVersionFile = Join-Path $dataDirectory   'address_file.etag'
$zipPath            = Join-Path $ScriptDirectory 'address-geocoder.zip'
$venvPath           = Join-Path $installFolder   '.venv'
$venvPython         = Join-Path $venvPath        'Scripts\python.exe'
$venvPip            = Join-Path $venvPath        'Scripts\pip.exe'
$activatePs1        = Join-Path $venvPath        'Scripts\Activate.ps1'
$configYml          = Join-Path $ScriptDirectory   'config.yml'
$configExample      = Join-Path $installFolder   'config_example.yml'
$toParquetPy        = Join-Path $installFolder   'csv_to_parquet.py'
$geocoderPy         = Join-Path $installFolder   'geocoder.py'

# Paths needed for installation
$wheelhouse    = Join-Path $ScriptDirectory 'wheelhouse'
$requirements1 = Join-Path $installFolder   '.\requirements.txt'

# GitHub Repo info
$repoURL = 'https://github.com/CityOfPhiladelphia/address-geocoder.git'
$owner = "CityOfPhiladelphia"
$repo = "address-geocoder"

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
        
        if ($localVersion -ne $remoteVersion) {
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

function installPython {
    Write-Host "Checking for Python 3.10 on this machine..."

    & py -3.10 --version > $null 2>&1
    if ($LASTEXITCODE -eq 0) {
        $ver = py -3.10 --version
        Write-Host "Python 3.10 is already available: $ver"
        return
    }

    Write-Host "Python 3.10 not found. Attempting installation via winget (source 'winget')..."

    $wingetArgs = @(
        "install",
        "-e",
        "--id","Python.Python.3.10",
        "--source","winget",
        "--accept-source-agreements",
        "--accept-package-agreements"
    )

    & winget @wingetArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "winget failed to install Python 3.10 (exit code $LASTEXITCODE)." -ForegroundColor Red
        Write-Host "You may need to install Python 3.10 manually from python.org, then re-run this script." -ForegroundColor Yellow
        Read-Host "Press Enter to exit"
        exit 1
    }

    # Refresh path after Python install
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")

    & py -3.10 --version > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "winget reported success, but 'py -3.10' is still not available." -ForegroundColor Red
        Write-Host "Please install Python 3.10 manually, ensure 'py -3.10' works, then re-run this script." -ForegroundColor Yellow
        Read-Host "Press Enter to exit"
        exit 1
    }

    $ver2 = py -3.10 --version
    Write-Host "Python 3.10 installation complete: $ver2"
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
    
    # Create venv if it doesn't exist
    if (-not (Test-Path $venvPath)) {
        Write-Host "Creating virtual environment..."
        try {
        $output = & uv venv $venvPath --python 3.10 2>&1
        }

        catch {
            #Output of this presents as an error for some reason even
            # when it executed successfully

        }
    } else {
        Write-Host "Virtual environment already exists."
    }

    if (Test-Path $requirements1) {
        Write-Host "Installing/updating packages..."
        
        $output = & uv pip install -r $requirements1 --python $venvPython 2>&1
        if ($LASTEXITCODE -ne 0) {
            $output | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        }
        
        Write-Host "Package installation complete!" -ForegroundColor Green
    }

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
            $null = git fetch origin 2>&1
            
            $localCommit = git rev-parse HEAD
            $remoteCommit = git rev-parse "origin/main"
            
            if ($localCommit -ne $remoteCommit) {
                Write-Host "Updates available. Pulling changes..."
                
                $status = git status --porcelain
                if ($status) {
                    Write-Host "Local changes detected. Stashing..."
                    $null = git stash push -m "Auto-stash before update" 2>&1
                    $stashed = $true
                } else {
                    $stashed = $false
                }
                
                $null = git pull origin "main" 2>&1
                
                # Restore stashed changes if we stashed them
                if ($stashed) {
                    Write-Host "Restoring local changes..."
                    $null = git stash pop 2>&1
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
            Pop-Location
            exit 1
        }
        
        Pop-Location
    } else {
        Write-Host "Repository not found. Cloning..."
        
        try {
            $null = git clone $repoURL $installFolder 2>&1
            
            Write-Host "Repository cloned successfully!" -ForegroundColor Green
            
            $script:RepoWasJustCloned = $true
        }
        catch {
            Write-Host "Failed to clone repository: $_" -ForegroundColor Red
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
        exit 1
    }

    if (-Not (Test-Path $addressFileCSV) -and -Not (Test-Path $addressFileParquet)) {
        Write-Host "Unzipping file..."
        decompressFile $addressFileGZ $addressFileCSV
        Remove-Item $addressFileGZ -Force
    }
   
    # Convert address file to parquet if no parquet file present
    if (-Not (Test-Path $addressFileParquet)) {   
        Write-Host "Converting address csv into a parquet file for speed and space optimization" -ForegroundColor Yellow
        
        & $venvPython -u $toParquetPy --input_path $addressFileCSV --output_path $addressFileParquet
        
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
installGit
installPython
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
Write-Host "`nRunning geocoder..." -ForegroundColor Green

try {
    # Use Start-Process to allow interactive prompts
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