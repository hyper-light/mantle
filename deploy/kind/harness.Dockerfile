# syntax=docker/dockerfile:1.7
# The transfer harness's image for the KIND cell (scripts/kind-cell.sh; docs/design/kubernetes.md):
# the mantle image's binary beside the stock S3 clients its HTTP door runs, boto3 pinned with every
# hash and the AWS CLI v2 at the version the scale runs used, and kubectl at the cluster's own
# version, its checksum checked, for the disruption. Test tools the owner approved for the stock
# clients' suite (2026-10-25); never a production image.
#
#   docker build -f deploy/kind/harness.Dockerfile --build-arg KUBECTL=v1.N.M -t mantle-harness:kind .

ARG MANTLE=mantle:kind
FROM ${MANTLE} AS mantle

FROM debian:trixie-slim
ARG KUBECTL
ARG AWS_CLI=2.34.18
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*
COPY crates/mantle/tests/support/boto3-requirements.txt /tmp/boto3-requirements.txt
RUN python3 -m venv /opt/stock \
    && /opt/stock/bin/python3 -m pip install --quiet --disable-pip-version-check --require-hashes \
       --only-binary :all: --no-deps -r /tmp/boto3-requirements.txt \
    && rm /tmp/boto3-requirements.txt
RUN arch="$(uname -m)" \
    && curl --fail --silent --show-error --location \
       "https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${AWS_CLI}.zip" -o /tmp/awscli.zip \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscli.zip
RUN test -n "${KUBECTL}" \
    && case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; *) exit 1 ;; esac \
    && curl --fail --silent --show-error --location \
       "https://dl.k8s.io/release/${KUBECTL}/bin/linux/${arch}/kubectl" -o /usr/local/bin/kubectl \
    && echo "$(curl --fail --silent --show-error --location \
       "https://dl.k8s.io/release/${KUBECTL}/bin/linux/${arch}/kubectl.sha256")  /usr/local/bin/kubectl" \
       | sha256sum --check --strict \
    && chmod 0755 /usr/local/bin/kubectl
COPY --from=mantle /mantle /usr/local/bin/mantle
COPY deploy/kind/disrupt.sh /usr/local/bin/disrupt
# The venv first, so the harness's `python3` is the one that has boto3.
ENV PATH=/opt/stock/bin:/usr/local/bin:/usr/bin:/bin
USER 65532:65532
ENV HOME=/tmp
WORKDIR /tmp
