resource "google_cloud_scheduler_job" "billing_reconciliation" {
  count = var.billing_reconciliation_enabled ? 1 : 0

  name             = "${var.worker_service_name}-billing-reconciliation"
  description      = "Closes wallet usage batches, recovers late provider usage, and cleans up legacy reservations."
  region           = var.region
  schedule         = var.billing_reconciliation_schedule
  time_zone        = "UTC"
  attempt_deadline = "300s"

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.worker.uri}/internal/billing/reconcile"
    body        = base64encode("{}")

    headers = {
      "Content-Type" = "application/json"
    }

    oidc_token {
      service_account_email = google_service_account.billing_reconciler.email
      audience              = google_cloud_run_v2_service.worker.uri
    }
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_service.worker,
    google_cloud_run_v2_service_iam_member.worker_billing_reconciler_invoker,
    google_firestore_index.billing_reservations_status_expires_at,
    google_firestore_index.billing_reservations_usage_reconciliation,
    google_firestore_index.billing_reservations_status_batch_created_at,
  ]
}

resource "google_firestore_index" "billing_reservations_status_expires_at" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "expires_at"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "billing_reservations_usage_reconciliation" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "usage_reconciliation_next_attempt_at"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "billing_reservations_status_batch_created_at" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "billing_batch_created_at"
    order      = "ASCENDING"
  }
}
