FROM nixos/nix:latest

RUN mkdir -p /etc/nix && echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

# The dev shell is built from a copy of the flake at /flake rather than from the
# workspace: /workspace is bind-mounted from the host during development, and a
# host-owned git checkout there makes Nix refuse to evaluate a flake inside it.
# Copying just the flake also caches dependency resolution independently of
# application code changes.
WORKDIR /flake
COPY flake.nix flake.lock ./
RUN nix develop --command true

# Git inside the container runs as root against host-owned files; without this
# every git command in the workspace fails the ownership check.
RUN nix develop --command git config --global --add safe.directory '*'

WORKDIR /workspace
COPY . .

ENTRYPOINT ["nix", "develop", "/flake", "--command"]
CMD ["bash"]
