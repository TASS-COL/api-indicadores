#!/usr/bin/env bash
set -euo pipefail

# ═════════════════════════════════════════════════════════════════════════════════
# Deploy Script para API-INDICATORS
# Descripción: Script de deployment automático para la API de indicadores financieros
# Uso: ./Deploy.sh [rama] [--skip-build] [--env-file]
# ═════════════════════════════════════════════════════════════════════════════════

REPO_DIR="${REPO_DIR:-.}"
REPO_URL="${REPO_URL:-}"
SSH_KEY="${SSH_KEY:-/home/ubuntu/.ssh/id_ed25519}"
COMPOSE_FILE="docker-compose.prod.yml"
DEPLOY_BRANCH="${1:-main}"
SKIP_BUILD="${2:---build}"
ENV_FILE="${REPO_DIR}/.env"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ── Funciones auxiliares ────────────────────────────────────────────────────────

log_info() {
  echo -e "${BLUE}ℹ ${1}${NC}"
}

log_success() {
  echo -e "${GREEN}✓ ${1}${NC}"
}

log_error() {
  echo -e "${RED}✗ ${1}${NC}"
}

log_warning() {
  echo -e "${YELLOW}⚠ ${1}${NC}"
}

# ── Configuración de Git SSH (si es necesario) ──────────────────────────────────

if [ -n "$REPO_URL" ] && [ ! -d "$REPO_DIR/.git" ]; then
  if [ ! -f "$SSH_KEY" ]; then
    log_warning "Clave SSH no encontrada en: $SSH_KEY"
    log_info "Se usará HTTPS si está disponible"
  else
    export GIT_SSH_COMMAND="ssh -i ${SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
  fi
fi

# ── [1] Validaciones previas ────────────────────────────────────────────────────

log_info "[1/7] Validando dependencias..."

# Validar Node.js/npm o yarn
if ! command -v node >/dev/null 2>&1; then
  log_error "Node.js no está instalado."
  exit 1
fi
log_success "Node.js $(node --version) ✓"

if ! command -v yarn >/dev/null 2>&1; then
  log_warning "yarn no encontrado, usando npm"
  PKG_MANAGER="npm"
else
  PKG_MANAGER="yarn"
  log_success "yarn $(yarn --version) ✓"
fi

# Validar Git
if ! command -v git >/dev/null 2>&1; then
  log_error "Git no está instalado."
  exit 1
fi
log_success "git $(git --version | awk '{print $3}') ✓"

# Validar Docker
if ! command -v docker >/dev/null 2>&1; then
  log_warning "Docker no está instalado. Se ejecutará sin containerización."
  DOCKER_AVAILABLE=false
else
  log_success "docker $(docker --version | awk '{print $3}' | cut -d, -f1) ✓"
  DOCKER_AVAILABLE=true
fi

# Validar Docker Compose (si Docker está disponible)
if [ "$DOCKER_AVAILABLE" = true ]; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
  else
    log_warning "Docker Compose no encontrado. Se ejecutará la API sin contenedores."
    DOCKER_AVAILABLE=false
  fi
fi

if [ "$DOCKER_AVAILABLE" = true ]; then
  log_success "Docker Compose ✓"
fi

# ── [2] Actualizar código ───────────────────────────────────────────────────────

log_info "[2/7] Actualizando código (rama: ${DEPLOY_BRANCH})..."

if [ -n "$REPO_URL" ]; then
  if [ ! -d "$REPO_DIR/.git" ]; then
    log_info "Clonando repositorio por primera vez..."
    git clone --branch "$DEPLOY_BRANCH" "$REPO_URL" "$REPO_DIR"
  else
    cd "$REPO_DIR"
    git fetch origin
    git checkout "$DEPLOY_BRANCH"
    git pull origin "$DEPLOY_BRANCH"
  fi
  cd "$REPO_DIR"
  COMMIT=$(git log -1 --format="%h %s")
  COMMIT_DATE=$(git log -1 --format="%ai")
  log_success "Código actualizado"
  log_info "  Commit: $COMMIT"
  log_info "  Fecha: $COMMIT_DATE"
else
  log_info "Usando código local en: $REPO_DIR"
  cd "$REPO_DIR"
  if [ -d ".git" ]; then
    COMMIT=$(git log -1 --format="%h %s")
    log_info "  Commit actual: $COMMIT"
  fi
fi

# ── [3] Verificar variables de entorno ──────────────────────────────────────────

log_info "[3/7] Verificando configuración..."

if [ ! -f "$ENV_FILE" ]; then
  log_error "No existe el archivo .env en: $ENV_FILE"
  log_info "Copia .env.example → .env y configura las variables de entorno"
  exit 1
fi
log_success ".env encontrado ✓"

# Validar variables críticas
REQUIRED_VARS=(
  "PGHOST"
  "PGPORT"
  "PGUSER"
  "PGPASSWORD"
  "PGDATABASE"
  "REDIS_URL"
  "JWT_SECRET"
  "API_KEY"
)

MISSING_VARS=()
for var in "${REQUIRED_VARS[@]}"; do
  if ! grep -q "^${var}=" "$ENV_FILE"; then
    MISSING_VARS+=("$var")
  fi
done

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
  log_warning "Variables de entorno faltantes: ${MISSING_VARS[*]}"
  log_info "Verifica que todas las variables requeridas estén en .env"
fi

log_success "Configuración validada ✓"

# ── [4] Instalar dependencias ───────────────────────────────────────────────────

log_info "[4/7] Instalando dependencias..."

if [ "$PKG_MANAGER" = "yarn" ]; then
  yarn install --frozen-lockfile
else
  npm ci
fi
log_success "Dependencias instaladas ✓"

# ── [5] Compilar TypeScript ─────────────────────────────────────────────────────

log_info "[5/7] Compilando TypeScript..."
$PKG_MANAGER run build
log_success "Compilación completada ✓"

# ── [6] Containers Docker ───────────────────────────────────────────────────────

if [ "$DOCKER_AVAILABLE" = true ]; then
  log_info "[6/7] Configurando contenedores Docker..."

  $COMPOSE_CMD -f "$COMPOSE_FILE" down --remove-orphans
  $COMPOSE_CMD -f "$COMPOSE_FILE" up -d --build

  log_success "Contenedores levantados ✓"
  log_info "Estado de contenedores:"
  $COMPOSE_CMD -f "$COMPOSE_FILE" ps
else
  log_warning "[6/7] Docker no disponible - omitiendo contenedores"
fi

# ── [7] Verificación final ──────────────────────────────────────────────────────

log_info "[7/7] Verificando estado de la aplicación..."

# Esperar a que la API esté lista
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
  if curl -sf http://localhost:3008/health >/dev/null 2>&1; then
    log_success "API disponible en http://localhost:3008 ✓"
    break
  fi
  ATTEMPT=$((ATTEMPT + 1))
  if [ $ATTEMPT -lt $MAX_ATTEMPTS ]; then
    echo -n "."
    sleep 1
  fi
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
  log_warning "No se pudo verificar la disponibilidad de la API (timeout)"
  log_info "Verifica los logs: docker compose -f $COMPOSE_FILE logs"
fi

# ── Resumen final ──────────────────────────────────────────────────────────────

echo ""
log_success "═══════════════════════════════════════════════════════════════"
log_success "Deploy completado exitosamente"
log_success "═══════════════════════════════════════════════════════════════"
echo ""
log_info "Información del deploy:"
log_info "  Rama: $DEPLOY_BRANCH"
log_info "  Directorio: $REPO_DIR"
log_info "  Package Manager: $PKG_MANAGER"
log_info "  Docker: $([ "$DOCKER_AVAILABLE" = true ] && echo "Activo" || echo "Desactivo")"
echo ""
log_info "Endpoints disponibles:"
log_info "  Health: http://localhost:3008/health"
log_info "  API: http://localhost:3008"
log_info "  Admin Queues: http://localhost:3008/admin/queues"
log_info "  Panel: http://localhost:3008/panel"
echo ""
log_info "Para ver logs:"
if [ "$DOCKER_AVAILABLE" = true ]; then
  log_info "  $COMPOSE_CMD -f $COMPOSE_FILE logs -f api"
else
  log_info "  Ver output de proceso"
fi
echo ""
