param(
    [Parameter(Mandatory = $true)]
    [string]$CredentialPath,

    [Parameter(Mandatory = $true)]
    [ValidateLength(1, 4096)]
    [string]$Message
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $CredentialPath -PathType Leaf)) {
    throw "Telegram credential file is unavailable."
}

$credential = Import-Clixml -LiteralPath $CredentialPath
if ($credential -isnot [System.Management.Automation.PSCredential]) {
    throw "Telegram credential file is invalid."
}
if ([string]::IsNullOrWhiteSpace($credential.UserName)) {
    throw "Telegram chat identity is missing."
}

$tokenPointer = [IntPtr]::Zero
$token = $null
try {
    $tokenPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR(
        $credential.Password
    )
    $token = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($tokenPointer)
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw "Telegram bot token is missing."
    }

    $payload = @{
        chat_id = $credential.UserName
        text = $Message
        link_preview_options = @{ is_disabled = $true }
    } | ConvertTo-Json -Compress -Depth 4

    try {
        $response = Invoke-RestMethod `
            -Method Post `
            -Uri ("https://api.telegram.org/bot{0}/sendMessage" -f $token) `
            -ContentType "application/json; charset=utf-8" `
            -Body $payload `
            -TimeoutSec 20
    }
    catch {
        # Do not expose an exception that could include the token-bearing URI.
        throw "Telegram Bot API request failed."
    }

    if ($null -eq $response -or $response.ok -ne $true) {
        throw "Telegram Bot API rejected the message."
    }
}
finally {
    if ($tokenPointer -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($tokenPointer)
    }
    $token = $null
    $credential = $null
}

