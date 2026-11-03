resource "google_service_account" "gateway" {
  account_id   = var.gateway_service_account_name
  display_name = "CEOsystem Agent Gateway"
}

resource "google_service_account" "worker" {
  account_id   = var.worker_service_account_name
  display_name = "CEOsystem Agent Persistence Worker"
}

resource "google_service_account" "eventarc" {
  account_id   = var.eventarc_service_account_name
  display_name = "CEOsystem Eventarc Trigger"
}

resource "google_service_account" "billing_reconciler" {
  account_id   = var.billing_reconciler_service_account_name
  display_name = "CEOsystem Billing Reconciler"
}

locals {
  gateway_roles = toset([
    "roles/cloudtrace.agent",
    "roles/datastore.user",
    "roles/logging.logWriter",
    "roles/pubsub.publisher",
    "roles/run.invoker",
  ])

  worker_roles = toset([
    "roles/cloudtrace.agent",
    "roles/datastore.user",
    "roles/logging.logWriter",
  ])

  eventarc_roles = toset([
    "roles/eventarc.eventReceiver",
    "roles/logging.logWriter",
  ])
}

resource "google_project_iam_member" "gateway_roles" {
  for_each = local.gateway_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.gateway.email}"
}

resource "google_project_iam_member" "worker_roles" {
  for_each = local.worker_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_project_iam_member" "eventarc_roles" {
  for_each = local.eventarc_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.eventarc.email}"
}
