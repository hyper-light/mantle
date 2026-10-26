#!/usr/bin/env bash
# The stock S3 clients the suite in crates/mantle/tests/stock_clients.rs runs, on a CI runner: the
# image's AWS CLI v2, installed from AWS's own installer where the image lacks it, and boto3 in a
# venv from crates/mantle/tests/support/boto3-requirements.txt, every package pinned with its hash.
# Both test tools only, approved by the owner on 2026-10-25. The venv goes first on the job's PATH,
# so the suite's `python3` (`python` on Windows) is the one that has boto3. Each version is printed.
set -euo pipefail

work="${RUNNER_TEMP:?a CI runner sets RUNNER_TEMP}"
case "$(uname -s)" in
  Linux*) os=linux ;;
  Darwin*) os=macos ;;
  MINGW*|MSYS*|CYGWIN*) os=windows ;;
  *) echo "stock-clients: an OS this script does not know: $(uname -s)" >&2; exit 1 ;;
esac

if ! command -v aws > /dev/null; then
  case "$os" in
    linux)
      arch="$(uname -m)"
      curl --fail --silent --show-error --location \
        "https://awscli.amazonaws.com/awscli-exe-linux-${arch}.zip" -o "$work/awscliv2.zip"
      unzip -q "$work/awscliv2.zip" -d "$work"
      sudo "$work/aws/install"
      ;;
    macos)
      curl --fail --silent --show-error --location \
        "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "$work/AWSCLIV2.pkg"
      sudo installer -pkg "$work/AWSCLIV2.pkg" -target /
      ;;
    windows)
      curl --fail --silent --show-error --location \
        "https://awscli.amazonaws.com/AWSCLIV2.msi" -o "$work/AWSCLIV2.msi"
      # msiexec returns before its install ends unless it is waited for.
      powershell.exe -NoProfile -Command \
        "Start-Process msiexec.exe -Wait -ArgumentList '/i','$(cygpath -w "$work/AWSCLIV2.msi")','/qn'"
      echo "C:\\Program Files\\Amazon\\AWSCLIV2" >> "$GITHUB_PATH"
      export PATH="/c/Program Files/Amazon/AWSCLIV2:$PATH"
      ;;
  esac
fi
aws --version

if [ "$os" = windows ]; then
  python=python
  bin="$work/stock-clients/Scripts"
else
  python=python3
  bin="$work/stock-clients/bin"
fi
"$python" -m venv "$work/stock-clients"
"$bin/$python" -m pip install --quiet --disable-pip-version-check --require-hashes --only-binary :all: --no-deps \
  -r crates/mantle/tests/support/boto3-requirements.txt
"$bin/$python" -c "import boto3, botocore, sys; print('boto3', boto3.__version__, 'botocore', botocore.__version__, 'python', sys.version.split()[0])"
if [ "$os" = windows ]; then
  cygpath -w "$bin" >> "$GITHUB_PATH"
else
  echo "$bin" >> "$GITHUB_PATH"
fi
