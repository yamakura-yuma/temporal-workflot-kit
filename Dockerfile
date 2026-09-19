FROM nixos/nix:latest

RUN mkdir -p /etc/nix && echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

WORKDIR /workspace

# Copy just the flake first so dependency resolution is cached independently
# of application code changes.
COPY flake.nix flake.lock ./
RUN nix develop --command true

COPY . .

ENTRYPOINT ["nix", "develop", "--command"]
CMD ["bash"]
