resource "google_compute_instance" "default" {
  name         = "automated-loadtest"
  machine_type = var.machine_type
  zone         = "us-central1-a"

  tags = ["automated-loadtest"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size = 100
      type = "pd-ssd"
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }
}
