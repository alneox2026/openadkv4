# OpenAI ADK Middleware Template

This template deploys an independently named Cloud Run gateway and persistence
worker for Google ADK agents that call OpenAI models through ADK's LiteLLM
integration. The gateway authenticates FlutterFlow/Firebase users, applies a
read-only minimum-balance gate, proxies buffered or SSE chat to the agent's
Cloud Run service, and publishes completed turns. The worker owns durable
transcripts, usage ledger records, batched wallet settlement, shortfall/debt
recovery, and provider-usage reconciliation.

The central Billing API remains the only Stripe/payment integration. This
middleware shares its configured customer-wallet, wallet-transaction, and
billing-period collections; turn reservations, transcripts, and agent ledgers
remain stack-specific. Collection names are Terraform-configured, not inferred
from this repository name. Review the deploy output before applying.

## Billing preflight invariant — keep this order

Treat this ordering as a hard billing and side-effect boundary:

- Authenticate and validate existing thread/session ownership using server-owned state only.
- Read the active wallet once and require at least `BILLING_MINIMUM_BALANCE_NANOS`
  (default `$0.05` / `50,000,000` nanos). If the check fails (including HTTP 402),
  return without creating a provider-side session or sending agent/model input.
- Only after the gate passes may the gateway create a provider-side session or submit
  agent/model input. This is a threshold gate, not a reservation or spend cap;
  it does not write to Firestore or protect that balance from concurrent turns.
- Publish a zero-value turn marker (`reserved_amount_nanos = 0`) with the completed
  event. The persistence worker creates the lifecycle record and owns all later
  wallet mutations.
- Keep buffered and streaming paths aligned; test that zero-credit requests return 402 with no
  provider-session or agent calls.

The gateway may generate local IDs before the balance check, but must not perform
provider-side side effects until the minimum-balance gate passes.

## Important: billing compatibility gate

The template intentionally fails closed on incomplete provider usage: it does
not invent a cache-write count or silently choose a fallback price. OpenAI
reports cached-input and cache-write tokens as separate usage fields. ADK's
typed `usage_metadata` schema does not include the cache-write field, so the
companion `adkOpenAI` example uses an adapter to preserve it in the event's
`custom_metadata.openai_usage.cache_write_tokens`; this middleware merges that
integer into its normal usage record.

This adapter is **not automatic for other ADK agents**. Deploy the updated
upstream agent image that includes the adapter (or implement an equivalent),
then verify a real streamed event carries
`customMetadata.openai_usage.cache_write_tokens`, including an explicit zero.
If a cache-eligible turn arrives without that count, the worker records a
known-cost lower bound and marks `estimated_cost_usd` unavailable. The worker
prices raw provider usage reports off the gateway request path, then keeps
retrying usage lookup from that exact backend invocation's persisted
session events while allowing subsequent requests whenever the current wallet
balance passes the minimum gate. Priced turns are queued as provisional estimates
and debited in bounded wallet-only batches (hourly by default, or earlier when
the configured batch limits are reached). The batch debit uses then-available
credit; any confirmed unpaid remainder is recorded as usage debt, not blended
with pending estimates. The shared Billing API's v4 top-up path must repay that
debt before adding new available credit. When provider usage is later corrected,
the worker settles only the difference from the already-accounted cost: an
increase charges available credit and adds any remainder to debt; a decrease
reduces debt first and credits only any excess back to the wallet. After the
configured retention period, unresolved usage is marked for manual review rather
than inventing a charge. Missing usage is not an admission lock: the gateway
rejects only when the wallet is missing/inactive, invalid, or below the configured
minimum balance. Keep this rule in place if billing reconciliation is changed.
The default worker schedule is once per hour (at minute 0), with provider lookups
starting after 5 minutes, exponentially backing off to 6 hours, and retrying for up
to 7 days. These values are configurable in Terraform. The agent Cloud Run
service must grant the worker identity permission to read its ADK session endpoint;
for Agent Runtime agents, grant the worker identity permission to read session
events as well. Do not route production traffic or accept live customer funds
until the deployed agent and middleware pass that end-to-end check. LiteLLM
response-result caching remains disabled for billable requests; OpenAI's
provider-side prompt caching remains enabled and is priced separately.

The `$0.05` gate is only a minimum-balance admission rule, not a per-turn spend
cap or escrow. An accepted tool-using ADK turn can cost more than the user's
remaining balance; the worker records the confirmed unpaid amount as debt. Set
appropriate model-call, prompt/output, and timeout limits in the upstream agent.
The Cloud Run middleware cannot impose output-token limits on a separately
deployed ADK agent. External tool/API costs are not included in model-token
charges unless separately reported and priced by the upstream integration.

## Pricing scope

The checked-in catalogs contain one supported model, `gpt-6-luna`, using
OpenAI's Standard/global rates. The app reports output tokens inclusive of
reasoning tokens, so reasoning is retained for audit but never added twice.
Input tokens are split into uncached, cached-hit, and cache-write portions when
the provider reports them. Long-context rates apply when a single model call's
input exceeds the configured 272,000-token threshold.

| Usage component | Up to 272,000 input tokens | Above 272,000 input tokens |
| --- | ---: | ---: |
| Uncached input, per 1M | $0.10 | $0.20 |
| Cached input, per 1M | $0.01 | $0.02 |
| Cache-write input, per 1M | $0.125 | $0.25 |
| Output including reasoning, per 1M | $0.50 | $0.75 |

Pricing is versioned in `config/billing.prod.yaml` and
`config/billing.test.yaml`. The worker validates the pricing catalog at startup;
unknown models fail closed when pricing usage. Verify provider rates and usage semantics before changing
either catalog. These values cover Standard/global token usage only; other
processing tiers, regions, hosted tools, and non-token fees are not supported.
See the [OpenAI GPT-6 Luna model and pricing page](https://developers.openai.com/api/docs/models/gpt-6-luna)
and [prompt caching guide](https://developers.openai.com/api/docs/guides/prompt-caching).

## Before deploying

1. Use a dedicated GCP project for production when possible. Select a unique
   `MIDDLEWARE_STACK_NAME`; the scripts derive stack-specific Cloud Run,
   service-account, Pub/Sub, image, and Terraform state names from it.
   Terraform remote state requires an existing private GCS bucket, defaulting
   to `<PROJECT_ID>-tfstate`. Set `TF_STATE_BUCKET` to your approved shared
   state bucket if it has another name; the script checks access before init.
2. Confirm the production agent URL and agent ID in
   `config/agents.prod.yaml`. The development registry is selected automatically
   for development/staging deployments.
3. Configure Firebase Auth, the permitted app origins, required GCP APIs/IAM,
   Cloud Monitoring alert channels, and access to the existing shared Firestore
   wallet collections. The deploy script requires alert channels in production.
4. Create a test wallet for each test user using the supplied provisioning
   helper; never let FlutterFlow write wallet or ledger documents. Set the
   v4 `billing_minimum_balance_nanos` gate to the intended threshold (default
   `$0.05`) and confirm the shared Billing API supports debt-first top-ups.
5. Keep OpenAI API keys in Secret Manager on the **agent** service. This
   middleware needs no OpenAI secret; it calls the agent service using the
   configured Cloud Run identity/auth policy.
6. Resolve the billing compatibility gate above and test the exact upstream
   usage event, batch close, low-balance shortfall/debt, debt-first top-up, and
   provider-cost correction behavior before accepting live funds.

## Cloud Shell deployment

From the root of the cloned repository:

```bash
export PROJECT_ID="YOUR_GCP_PROJECT_ID"
export REGION="us-central1"
export MIDDLEWARE_STACK_NAME="openai-agents"
export MIDDLEWARE_IMAGE_PREFIX="openai-agents"

bash ./scripts/cloudshell_build_middleware.sh

export DEPLOYMENT_ENV="development"
export ALLOWED_ORIGINS="https://ceoappdev.flutterflow.app"
bash ./scripts/cloudshell_deploy_middleware.sh
```

The build script uses temporary Cloud Build config files and creates separate
gateway and worker images. The deploy script pins the resolved image digests,
uses `stacks/<stack>/middleware` for Terraform state, displays a plan, and
requires explicit confirmation. Do not reuse another middleware's state prefix.

For production, use the production project, set
`DEPLOYMENT_ENV=production`, set `ALLOWED_ORIGINS` to the real production web
origins, provide `ALERT_NOTIFICATION_CHANNELS_JSON` with at least one
Cloud Monitoring channel, and review `config/agents.prod.yaml` and the full
Terraform plan. Only after testing the upstream ADK usage contract, export
`OPENAI_USAGE_ACCOUNTING_REVIEWED=true`; production deployment otherwise stops.
The script also rejects the development placeholder project and known
placeholder agent URLs. Do not bypass those guards to make a plan pass.

## API

- `GET /health` — liveness check
- `GET /ready` — readiness check
- `POST /v1/agents/{agent_id}/chat` — buffered response
- `POST /v1/agents/{agent_id}/chat/stream` — Server-Sent Events response

Requests require Firebase authentication according to each registry entry.
The client supplies a message and optional session/thread metadata; user identity,
zero-value billing marker identity, and completed-turn event IDs are server-owned.

## Validation

Run the template tests with Python 3.11 or newer:

```bash
python -m pytest tests/ -q
```

Check deployment scripts with Bash syntax validation:

```bash
bash -n scripts/cloudshell_build_middleware.sh
bash -n scripts/cloudshell_deploy_middleware.sh
```

Terraform plans and actual GCP permissions/resources must still be checked in
the target project; passing local tests does not certify a live deployment.
