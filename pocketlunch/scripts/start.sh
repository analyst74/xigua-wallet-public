#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
ENV_EXAMPLE="${ROOT_DIR}/.env.example"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
CREATED_ENV=0

rawurlencode() {
  local input="${1}"
  local output=""
  local i char hex

  for ((i = 0; i < ${#input}; i++)); do
    char="${input:i:1}"
    case "${char}" in
      [a-zA-Z0-9.~_-]) output+="${char}" ;;
      *)
        printf -v hex '%%%02X' "'${char}"
        output+="${hex}"
        ;;
    esac
  done

  printf '%s' "${output}"
}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not installed."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required but not available."
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${ENV_EXAMPLE}" "${ENV_FILE}"
  chmod 600 "${ENV_FILE}"
  echo "Created ${ENV_FILE} from template."
  CREATED_ENV=1
fi

get_env_value() {
  local key="${1}"
  awk -F= -v k="${key}" '$1==k {print substr($0, index($0, "=") + 1)}' "${ENV_FILE}" | tail -n 1
}

token_from_file="$(get_env_value "LUNCH_MONEY_API_TOKEN")"
db_password_from_file="$(get_env_value "POSTGRES_PASSWORD")"
db_password_urlenc_from_file="$(get_env_value "POSTGRES_PASSWORD_URLENC")"

if [[ "${CREATED_ENV}" -eq 1 ]]; then
  token_value="${token_from_file:-${LUNCH_MONEY_API_TOKEN:-}}"
  if [[ -z "${token_value}" ]]; then
    read -r -s -p "Enter LUNCH_MONEY_API_TOKEN: " token_value
    echo
  fi

  if [[ -z "${token_value}" ]]; then
    echo "LUNCH_MONEY_API_TOKEN is required."
    exit 1
  fi

  db_password="${db_password_from_file:-}"
  if [[ -z "${db_password}" || "${db_password}" == "change_me" ]]; then
    if command -v openssl >/dev/null 2>&1; then
      db_password="$(openssl rand -base64 24 | tr -d '\n')"
    else
      db_password="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32)"
    fi
  fi

  db_password_urlenc="$(rawurlencode "${db_password}")"

  tmp_env="$(mktemp)"
  awk -F= -v token="${token_value}" -v pgpass="${db_password}" -v pgpassenc="${db_password_urlenc}" '
BEGIN { t=0; p=0; pe=0 }
$1=="LUNCH_MONEY_API_TOKEN" { print "LUNCH_MONEY_API_TOKEN=" token; t=1; next }
$1=="POSTGRES_PASSWORD" { print "POSTGRES_PASSWORD=" pgpass; p=1; next }
$1=="POSTGRES_PASSWORD_URLENC" { print "POSTGRES_PASSWORD_URLENC=" pgpassenc; pe=1; next }
{ print $0 }
END {
  if (t==0) print "LUNCH_MONEY_API_TOKEN=" token;
  if (p==0) print "POSTGRES_PASSWORD=" pgpass;
  if (pe==0) print "POSTGRES_PASSWORD_URLENC=" pgpassenc;
}
' "${ENV_FILE}" > "${tmp_env}"
  mv "${tmp_env}" "${ENV_FILE}"
  chmod 600 "${ENV_FILE}"
else
  if [[ -z "${token_from_file}" ]]; then
    echo "Missing LUNCH_MONEY_API_TOKEN in ${ENV_FILE}."
    echo "Set it once in ${ENV_FILE}; start.sh will not rewrite existing env files."
    exit 1
  fi

  if [[ -z "${db_password_from_file}" ]]; then
    echo "Missing POSTGRES_PASSWORD in ${ENV_FILE}."
    echo "Set it once in ${ENV_FILE}; start.sh will not rewrite existing env files."
    exit 1
  fi

  if [[ -z "${db_password_urlenc_from_file}" ]]; then
    echo "Missing POSTGRES_PASSWORD_URLENC in ${ENV_FILE}."
    echo "Set it once in ${ENV_FILE}; start.sh will not rewrite existing env files."
    exit 1
  fi

  expected_urlenc="$(rawurlencode "${db_password_from_file}")"
  if [[ "${db_password_urlenc_from_file}" != "${expected_urlenc}" ]]; then
    echo "POSTGRES_PASSWORD_URLENC does not match POSTGRES_PASSWORD in ${ENV_FILE}."
    echo "Update POSTGRES_PASSWORD_URLENC to: ${expected_urlenc}"
    exit 1
  fi
fi

docker compose \
  --env-file "${ENV_FILE}" \
  -f "${COMPOSE_FILE}" \
  up -d --build

echo
echo "Stack started:"
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" ps
echo
echo "Logs:"
echo "  docker compose --env-file \"${ENV_FILE}\" -f \"${COMPOSE_FILE}\" logs -f"
