# Get the directory of the currently executing script
###
$ScriptDirectory = (Split-Path -Parent (Get-Process -Id $PID).Path) 
$ScriptDirectory = (Resolve-Path -LiteralPath $ScriptDirectory).ProviderPath

# Paths needed for script
$installFolder = Join-Path $ScriptDirectory 'address-geocoder-main'
$zipPath       = Join-Path $ScriptDirectory 'address-geocoder.zip'
$venvPath      = Join-Path $installFolder   '.venv'
$venvPython    = Join-Path $venvPath        'Scripts\python.exe'
$venvPip       = Join-Path $venvPath        'Scripts\pip.exe'
$activatePs1   = Join-Path $venvPath        'Scripts\Activate.ps1'
$configYml     = Join-Path $ScriptDirectory 'config.yml'
$configExample = Join-Path $installFolder   'config_example.yml'
$geocoderPy    = Join-Path $installFolder   'geocoder.py'

# Paths needed for installation
$wheelhouse    = Join-Path $ScriptDirectory 'wheelhouse'
$requirements1 = Join-Path $installFolder   '.\requirements.txt'

# GitHub Repo
$repoURL = 'https://github.com/CityOfPhiladelphia/address-geocoder.git'

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

function createVenvAndConfig {
    Write-Host "Setting up virtual environment and packages..."
    
    # Create venv if it doesn't exist
    if (-not (Test-Path $venvPath)) {
        Write-Host "Creating virtual environment..."
        py -3.10 -m venv $venvPath
    } else {
        Write-Host "Virtual environment already exists."
    }

    # Upgrade pip and install -- quiet suppresses notices
    # so user isn't spammed with package info
    Write-Host "Upgrading pip and installing setuptools/wheel..."
    $null = & $venvPython -m pip install --upgrade pip setuptools wheel --quiet 2>&1

    if (Test-Path $requirements1) {
        Write-Host "Installing required packages..."
        
        # Read requirements.txt and separate GitHub packages from regular packages
        $requirements = Get-Content $requirements1
        $githubPackages = @()
        $regularPackages = @()
        
        foreach ($line in $requirements) {
            $line = $line.Trim()
            if ($line -and -not $line.StartsWith("#")) {
                if ($line -match "github\.com" -or $line -match "git\+") {
                    $githubPackages += $line
                } else {
                    $regularPackages += $line
                }
            }
        }
        
        # Install regular packages
        if ($regularPackages.Count -gt 0) {
            Write-Host "Installing regular packages..."
            $tempReqFile = Join-Path $env:TEMP "temp_requirements.txt"
            $regularPackages | Out-File -FilePath $tempReqFile -Encoding UTF8
            
            # Only show output if there's an error
            $output = & $venvPip install -r $tempReqFile --quiet --quiet 2>&1
            if ($LASTEXITCODE -ne 0) {
                $output | Where-Object { 
                    $_ -match "error:" -and 
                    $_ -notmatch "\[notice\]" -and 
                    $_ -notmatch "WARNING"
                } | ForEach-Object { Write-Host $_ -ForegroundColor Red }
            }
            Remove-Item $tempReqFile -ErrorAction SilentlyContinue
        }
        
        # Install GitHub packages
        if ($githubPackages.Count -gt 0) {
            Write-Host "Checking GitHub-based packages..."
            foreach ($package in $githubPackages) {
                # Extract package name
                $packageName = ""
                if ($package -match "([^/]+?)(?:\.git)?(?:@|$)") {
                    $packageName = $Matches[1].Split('@')[0].Trim()
                }
                
                # Check if package is already installed
                $checkInstalled = & $venvPip show $packageName 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "  $packageName is already installed" -ForegroundColor Green
                    continue
                }
                
                Write-Host "  Installing: $package"
                
                # Clean up the package URL - ensure it has git+ prefix
                $cleanPackage = $package
                if ($package -match "passyunk" -and $package -notmatch "^git\+") {
                    $cleanPackage = "git+https://github.com/CityOfPhiladelphia/passyunk.git"
                } elseif ($package -notmatch "^git\+" -and $package -match "github\.com") {
                    $cleanPackage = "git+$package"
                }
                
                $output = & $venvPip install $cleanPackage --no-cache-dir --quiet --quiet 2>&1
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "  Successfully installed: $package" -ForegroundColor Green
                } else {
                    # Only show actual errors
                    $realErrors = $output | Where-Object { 
                        ($_ -match "fatal:" -or $_ -match "Failed") -and 
                        $_ -notmatch "\[notice\]" -and
                        $_ -notmatch "WARNING" -and
                        $_ -notmatch "Running command git"
                    }
                    
                    if ($realErrors) {
                        Write-Host "  Failed to install $package" -ForegroundColor Yellow
                        $realErrors | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
                    }
                    
                    Write-Host "  Trying alternative method..." -ForegroundColor Yellow
                    
                    # Try cloning and installing locally as fallback
                    try {
                        $tempDir = Join-Path $env:TEMP "temp_git_package"
                        if (Test-Path $tempDir) { Remove-Item $tempDir -Recurse -Force }
                        
                        $gitUrl = $cleanPackage -replace "^git\+", ""
                        $null = git clone $gitUrl $tempDir 2>&1
                        Push-Location $tempDir
                        $null = & $venvPip install . --quiet --quiet 2>&1
                        Pop-Location
                        Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
                        
                        Write-Host "  Successfully installed using alternative method" -ForegroundColor Green
                    }
                    catch {
                        Write-Host "  Alternative installation also failed. Skipping this package." -ForegroundColor Red
                    }
                }
            }
        }
        
        Write-Host "Package installation complete!"
    } else {
        Write-Host "No requirements.txt found at: $requirements1" -ForegroundColor Yellow
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
                    git stash push -m "Auto-stash before update"
                }
                
                $null = git pull origin "main" 2>&1
                
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
            $cloneRepo = git clone $repoURL $installFolder 2>$null
            
            Write-Host "Repository cloned successfully!" -ForegroundColor Green
            
            $script:RepoWasJustCloned = $true
        }
        catch {
            Write-Host "Failed to clone repository: $_" -ForegroundColor Red
            exit 1
        }
    }
}

$script:RepoWasJustCloned = $false
$script:RepoWasUpdated = $false

# Execute installation steps
installGit
installPython
cloneOrUpdate
createVenvAndConfig

# If repo was just cloned or updated, user needs to configure before running
if ($script:RepoWasJustCloned) {
    Write-Host "`n========================================" -ForegroundColor Yellow
    Write-Host "FIRST TIME SETUP COMPLETE" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host "Please edit the config.yml file with your settings before running the geocoder."
    Write-Host "Config file location: $configYml"
    Write-Host "`nRun this script again after configuring to start the geocoder."
    Read-Host "`nPress Enter to exit"
    exit 0
} elseif ($script:RepoWasUpdated) {
    Write-Host "`n========================================" -ForegroundColor Yellow
    Write-Host "REPOSITORY UPDATED" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host "The repository was updated. Please verify your config.yml settings."
    Write-Host "Config file location: $configYml"
    Write-Host "`nRun this script again to start the geocoder."
    Read-Host "`nPress Enter to exit"
    exit 0
} else {
    # Repository exists and is up to date, run the program
    Write-Host "`nRunning geocoder..." -ForegroundColor Green

    try {
        Start-Process -FilePath $venvPython `
            -ArgumentList @("-u", $geocoderPy) `
            -WorkingDirectory $ScriptDirectory `
            -NoNewWindow `
            -Wait
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