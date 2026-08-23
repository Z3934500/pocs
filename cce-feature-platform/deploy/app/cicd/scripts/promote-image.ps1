param(
    [Parameter(Mandatory=$true)][string]$SourceImage,
    [Parameter(Mandatory=$true)][string]$TargetImage,
    [switch]$Push
)

$ErrorActionPreference = "Stop"

Write-Host "Pulling source image: $SourceImage"
docker pull $SourceImage

Write-Host "Tagging target image: $TargetImage"
docker tag $SourceImage $TargetImage

if ($Push) {
    Write-Host "Pushing target image: $TargetImage"
    docker push $TargetImage
} else {
    Write-Host "Push skipped. Re-run with -Push to publish."
}

Write-Host "Image promotion completed."
