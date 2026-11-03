#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Standalone Deployment Template for OpenAI-powered Cloud Run ADK Agents
# ==============================================================================

PROJECT_ID="${PROJECT_ID:-ceo-dev123}"
REGION="${REGION:-us-central1}"
REPOSITORY="${REPOSITORY:-ceosystem}"

# Set the name of your agent and its local source directory
AGENT_NAME="${1:-maximus-2026}"
AGENT_DIR="${2:-./}"
MODEL_NAME="${3:-gpt-6-luna}"

echo "================================================================="
echo " Deploying OpenAI Cloud Run ADK Agent"
echo " Agent Name : ${AGENT_NAME}"
echo " Source Dir : ${AGENT_DIR}"
echo " Model      : ${MODEL_NAME}"
echo " Project ID : ${PROJECT_ID}"
echo " Region     : ${REGION}"
echo "================================================================="

gcloud config set project "${PROJECT_ID}"

TAG="$(date +%Y%m%d%H%M%S)"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${AGENT_NAME}:${TAG}"
LATEST_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${AGENT_NAME}:latest"

echo "--> [1/2] Building Container Image in Artifact Registry..."
gcloud builds submit "${AGENT_DIR}" \
  --config=<(cat <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: ["build", "-t", "${IMAGE_URI}", "-t", "${LATEST_URI}", "."]
images:
- "${IMAGE_URI}"
- "${LATEST_URI}"
EOF
) --project="${PROJECT_ID}"

# Prepare environment variables
ENV_VARS="PROJECT_ID=${PROJECT_ID},GOOGLE_CLOUD_PROJECT=${PROJECT_ID},MODEL_NAME=${MODEL_NAME}"
if [ -n "${OPENAI_API_KEY:-}" ]; then
  ENV_VARS="${ENV_VARS},OPENAI_API_KEY=${OPENAI_API_KEY}"
fi

echo "--> [2/2] Deploying to Cloud Run with Production Sizing..."
gcloud run deploy "${AGENT_NAME}" \
  --image="${IMAGE_URI}" \
  --region="${REGION}" \
  --platform=managed \
  --allow-unauthenticated \
  --cpu=2 \
  --memory=2Gi \
  --concurrency=80 \
  --min-instances=1 \
  --max-instances=30 \
  --timeout=300 \
  --set-env-vars="${ENV_VARS}" \
  --project="${PROJECT_ID}"

SERVICE_URL="$(gcloud run services describe "${AGENT_NAME}" --region="${REGION}" --format="value(status.url)" --project="${PROJECT_ID}")"

echo "================================================================="
echo " Agent Deployed Successfully to Cloud Run!"
echo " Service URL: ${SERVICE_URL}"
echo ""
echo " Register this agent in config/agents.prod.yaml:"
echo "---------------------------------------------------"
echo "agents:"
echo "  ${AGENT_NAME//-/_}:"
echo "    agent_id: ${AGENT_NAME//-/_}"
echo "    backend: cloud_run_adk"
echo "    model: ${MODEL_NAME}"
echo "    base_url: ${SERVICE_URL}"
echo "    app_name: app"
echo "    region: ${REGION}"
echo "    streaming_enabled: true"
echo "    persistence_enabled: true"
echo "    auth_policy: firebase"
echo "    runtime_session_cleanup: cloud_run_adk"
echo "================================================================="
