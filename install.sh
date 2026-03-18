#!/usr/bin/env bash

set -euo pipefail

readonly DEFAULT_REPO="mydearniko/zstdmt"
readonly DEFAULT_PREFIX="/usr/local"
readonly PROGRAMS=(
  brotli-mt
  lizard-mt
  lz4-mt
  lz5-mt
  zstd-mt
  snappy-mt
  lzfse-mt
)
readonly ALIASES=(
  "unbrotli-mt:brotli-mt"
  "brotlicat-mt:brotli-mt"
  "unlizard-mt:lizard-mt"
  "lizardcat-mt:lizard-mt"
  "unlz4-mt:lz4-mt"
  "lz4cat-mt:lz4-mt"
  "unlz5-mt:lz5-mt"
  "lz5cat-mt:lz5-mt"
  "unzstd-mt:zstd-mt"
  "zstdcat-mt:zstd-mt"
  "unsnappy-mt:snappy-mt"
  "snappycat-mt:snappy-mt"
  "unlzfse-mt:lzfse-mt"
  "lzfsecat-mt:lzfse-mt"
)

REPO="${DEFAULT_REPO}"
PREFIX="${DEFAULT_PREFIX}"
ACTION="install"
TAG_OVERRIDE=""
SUDO=()
DOWNLOAD_PACKAGE_DIR=""

usage() {
  cat <<'EOF'
Usage:
  install.sh [install] [--prefix DIR] [--repo OWNER/REPO] [--tag TAG]
  install.sh update [--prefix DIR] [--repo OWNER/REPO] [--tag TAG]
  install.sh remove [--prefix DIR]

Commands:
  install   Install the matching release for this machine (default command).
  update    Install the newest matching release and replace older installs.
  remove    Remove the managed zstdmt installation from the prefix.

Options:
  --prefix DIR       Install under DIR (default: /usr/local).
  --repo OWNER/REPO  Release source repository (default: mydearniko/zstdmt).
  --tag TAG          Install a specific release tag instead of the latest one.
  -h, --help         Show this help text.

Examples:
  ./install.sh
  ./install.sh install --prefix "$HOME/.local"
  ./install.sh update
  ./install.sh remove
EOF
}

log() {
  printf '%s\n' "$*" >&2
}

warn() {
  printf 'warning: %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

first_existing_parent() {
  local path="$1"

  while [ ! -e "$path" ]; do
    path="$(dirname "$path")"
  done

  printf '%s\n' "$path"
}

setup_privileges() {
  local path

  for path in "${ROOT_DIR}" "${BIN_DIR}"; do
    if [ ! -w "$(first_existing_parent "$path")" ]; then
      if [ "${EUID:-$(id -u)}" -eq 0 ]; then
        return
      fi
      command -v sudo >/dev/null 2>&1 || die "write access to ${PREFIX} is required; rerun as root or use --prefix"
      SUDO=(sudo)
      return
    fi
  done
}

run_root() {
  if [ "${#SUDO[@]}" -gt 0 ]; then
    "${SUDO[@]}" "$@"
  else
    "$@"
  fi
}

detect_platform() {
  local os arch

  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      PLATFORM_OS="linux"
      ;;
    Darwin)
      PLATFORM_OS="macos"
      ;;
    *)
      die "unsupported operating system: ${os}"
      ;;
  esac

  case "$arch" in
    x86_64|amd64)
      PLATFORM_ARCH="x86_64"
      ;;
    i386|i686)
      PLATFORM_ARCH="x86"
      ;;
    aarch64|arm64)
      PLATFORM_ARCH="arm64"
      ;;
    armv7l|armv7|armhf)
      PLATFORM_ARCH="armv7"
      ;;
    *)
      die "unsupported architecture: ${arch}"
      ;;
  esac

  ARCHIVE_NAME="zstdmt-${PLATFORM_OS}-${PLATFORM_ARCH}"
  ARCHIVE_FILE="${ARCHIVE_NAME}.tar.gz"
}

resolve_latest_stable_tag() {
  local response http_code effective_url

  response="$(
    curl -sSIL -o /dev/null -w '%{http_code} %{url_effective}' \
      "https://github.com/${REPO}/releases/latest"
  )"
  http_code="${response%% *}"
  effective_url="${response#* }"

  if [ "${http_code}" = "200" ] && [[ "${effective_url}" == *"/releases/tag/"* ]]; then
    printf '%s\n' "${effective_url##*/}"
    return 0
  fi

  return 1
}

resolve_latest_release_tag() {
  local api_url response tag

  if tag="$(resolve_latest_stable_tag)"; then
    printf '%s\n' "${tag}"
    return 0
  fi

  api_url="https://api.github.com/repos/${REPO}/releases?per_page=1"
  response="$(curl -fsSL "${api_url}")" || return 1
  tag="$(
    printf '%s\n' "${response}" |
      sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]*\)",[[:space:]]*$/\1/p' |
      head -n 1
  )"
  [ -n "${tag}" ] || return 1
  printf '%s\n' "${tag}"
}

fetch_file() {
  local url="$1" output="$2"

  curl --fail --location --retry 3 --silent --show-error \
    --output "${output}" "${url}"
}

verify_checksum() {
  local workdir="$1" archive_file="$2" checksum_file="$3" normalized_checksum

  normalized_checksum="${workdir}/${archive_file}.sha256.check"
  sed "s#  .*#  ${archive_file}#" "${workdir}/${checksum_file}" > "${normalized_checksum}"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "${workdir}" && sha256sum -c "$(basename "${normalized_checksum}")" >&2)
  elif command -v shasum >/dev/null 2>&1; then
    (cd "${workdir}" && shasum -a 256 -c "$(basename "${normalized_checksum}")" >&2)
  else
    die "missing checksum tool: need sha256sum or shasum"
  fi
}

path_belongs_to_install() {
  local path="$1" target

  [ -L "${path}" ] || return 1
  target="$(readlink "${path}")"
  case "${target}" in
    "${ROOT_DIR}"/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

assert_link_is_safe() {
  local path="$1"

  if [ -L "${path}" ]; then
    path_belongs_to_install "${path}" || die "${path} already exists and points outside ${ROOT_DIR}"
    return 0
  fi

  [ ! -e "${path}" ] || die "${path} already exists and is not managed by this installer"
}

install_link() {
  local source="$1" destination="$2"

  assert_link_is_safe "${destination}"
  run_root ln -sfn "${source}" "${destination}"
}

write_metadata() {
  local release_tag="$1" asset_name="$2" metadata_file tmpfile

  metadata_file="${ROOT_DIR}/install.conf"
  tmpfile="$(mktemp)"
  cat > "${tmpfile}" <<EOF
repo=${REPO}
tag=${release_tag}
asset=${asset_name}
prefix=${PREFIX}
EOF
  run_root install -m 0644 "${tmpfile}" "${metadata_file}"
  rm -f "${tmpfile}"
}

cleanup_old_releases() {
  local keep_tag="$1" release_base release_path release_name

  release_base="${ROOT_DIR}/releases"
  [ -d "${release_base}" ] || return 0

  while IFS= read -r -d '' release_path; do
    release_name="$(basename "${release_path}")"
    if [ "${release_name}" != "${keep_tag}" ]; then
      run_root rm -rf "${release_path}"
    fi
  done < <(find "${release_base}" -mindepth 1 -maxdepth 1 -type d -print0)
}

read_installed_tag() {
  local metadata_file

  metadata_file="${ROOT_DIR}/install.conf"
  [ -r "${metadata_file}" ] || return 1
  sed -n 's/^tag=//p' "${metadata_file}" | head -n 1
}

download_and_extract_release() {
  local release_tag="$1" archive_url checksum_url extract_dir package_dir bin

  TMPDIR_RELEASE="$(mktemp -d)"
  trap 'rm -rf "${TMPDIR_RELEASE}"' EXIT

  archive_url="https://github.com/${REPO}/releases/download/${release_tag}/${ARCHIVE_FILE}"
  checksum_url="${archive_url}.sha256"

  log "Downloading ${ARCHIVE_FILE} from ${REPO} ${release_tag}"
  fetch_file "${archive_url}" "${TMPDIR_RELEASE}/${ARCHIVE_FILE}"
  fetch_file "${checksum_url}" "${TMPDIR_RELEASE}/${ARCHIVE_FILE}.sha256"
  verify_checksum "${TMPDIR_RELEASE}" "${ARCHIVE_FILE}" "${ARCHIVE_FILE}.sha256"

  extract_dir="${TMPDIR_RELEASE}/extract"
  mkdir -p "${extract_dir}"
  tar -xzf "${TMPDIR_RELEASE}/${ARCHIVE_FILE}" -C "${extract_dir}"
  package_dir="$(find "${extract_dir}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [ -n "${package_dir}" ] || die "release archive did not contain a package directory"

  for bin in "${PROGRAMS[@]}"; do
    [ -f "${package_dir}/bin/${bin}" ] || die "release archive is missing ${bin}"
  done

  DOWNLOAD_PACKAGE_DIR="${package_dir}"
}

perform_install() {
  local requested_tag="$1" release_tag previous_tag package_dir release_dir bin alias_name target_name

  detect_platform

  if [ -n "${requested_tag}" ]; then
    release_tag="${requested_tag}"
  else
    release_tag="$(resolve_latest_release_tag)" || die "unable to resolve a published release for ${REPO}"
  fi

  previous_tag="$(read_installed_tag || true)"
  download_and_extract_release "${release_tag}"
  package_dir="${DOWNLOAD_PACKAGE_DIR}"
  release_dir="${ROOT_DIR}/releases/${release_tag}"

  setup_privileges
  run_root mkdir -p "${ROOT_DIR}/releases" "${BIN_DIR}"
  run_root rm -rf "${release_dir}"
  run_root mkdir -p "${release_dir}"
  run_root cp -R "${package_dir}/." "${release_dir}/"
  run_root chmod 0755 "${release_dir}/bin"/*
  run_root ln -sfn "${release_dir}" "${CURRENT_DIR}"

  for bin in "${PROGRAMS[@]}"; do
    install_link "${CURRENT_DIR}/bin/${bin}" "${BIN_DIR}/${bin}"
  done

  for alias_name in "${ALIASES[@]}"; do
    target_name="${alias_name#*:}"
    alias_name="${alias_name%%:*}"
    install_link "${CURRENT_DIR}/bin/${target_name}" "${BIN_DIR}/${alias_name}"
  done

  write_metadata "${release_tag}" "${ARCHIVE_FILE}"
  cleanup_old_releases "${release_tag}"

  if [ -n "${previous_tag}" ] && [ "${previous_tag}" != "${release_tag}" ]; then
    log "Updated ${REPO} to ${release_tag} in ${PREFIX}"
  else
    log "Installed ${REPO} ${release_tag} into ${PREFIX}"
  fi
}

perform_update() {
  local latest_tag installed_tag

  detect_platform
  if [ -n "${TAG_OVERRIDE}" ]; then
    latest_tag="${TAG_OVERRIDE}"
  else
    latest_tag="$(resolve_latest_release_tag)" || die "unable to resolve a published release for ${REPO}"
  fi
  installed_tag="$(read_installed_tag || true)"

  if [ -n "${installed_tag}" ] && [ "${installed_tag}" = "${latest_tag}" ]; then
    log "${REPO} ${latest_tag} is already installed in ${PREFIX}"
    return 0
  fi

  perform_install "${latest_tag}"
}

perform_remove() {
  local name path

  setup_privileges

  for name in "${PROGRAMS[@]}"; do
    path="${BIN_DIR}/${name}"
    if path_belongs_to_install "${path}"; then
      run_root rm -f "${path}"
    elif [ -L "${path}" ]; then
      warn "keeping ${path}; it is not managed by ${ROOT_DIR}"
    fi
  done

  for name in "${ALIASES[@]}"; do
    path="${BIN_DIR}/${name%%:*}"
    if path_belongs_to_install "${path}"; then
      run_root rm -f "${path}"
    elif [ -L "${path}" ]; then
      warn "keeping ${path}; it is not managed by ${ROOT_DIR}"
    fi
  done

  if [ -e "${ROOT_DIR}" ] || [ -L "${ROOT_DIR}" ]; then
    run_root rm -rf "${ROOT_DIR}"
  fi

  log "Removed zstdmt from ${PREFIX}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    install|update|remove)
      ACTION="$1"
      ;;
    --prefix)
      [ $# -ge 2 ] || die "--prefix requires a value"
      PREFIX="$2"
      shift
      ;;
    --repo)
      [ $# -ge 2 ] || die "--repo requires a value"
      REPO="$2"
      shift
      ;;
    --tag)
      [ $# -ge 2 ] || die "--tag requires a value"
      TAG_OVERRIDE="$2"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

require_cmd curl
require_cmd tar
require_cmd find
require_cmd mktemp
require_cmd readlink

PREFIX="${PREFIX%/}"
BIN_DIR="${PREFIX}/bin"
ROOT_DIR="${PREFIX}/lib/zstdmt"
CURRENT_DIR="${ROOT_DIR}/current"

case "${ACTION}" in
  install)
    perform_install "${TAG_OVERRIDE}"
    ;;
  update)
    perform_update
    ;;
  remove)
    perform_remove
    ;;
  *)
    die "unsupported action: ${ACTION}"
    ;;
esac
