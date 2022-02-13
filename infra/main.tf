terraform {
  required_providers {
    github = {
      source  = "integrations/github"
      version = "~> 4.0"
    }
    heroku = {
      source  = "heroku/heroku"
      version = ">=4.8.0"
    }
    namecheap = {
      source  = "namecheap/namecheap"
      version = ">= 2.0.0"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 3.0"
    }
  }
}

# Github repo and Heroku app --------------------------------
provider "github" {
  token = var.github_token
}
resource "github_repository" "my_repo" {
  name       = var.repo_name
  visibility = "private"
}
resource "github_branch_default" "default" {
  repository = github_repository.my_repo.name
  branch     = "main"
}

provider "heroku" {
  email   = var.heroku_username
  api_key = var.heroku_api_key
}
resource "heroku_app" "my_app" {
  name   = var.app_name
  region = "us"
  stack  = "container"

  config_vars = {
  }
}

resource "github_actions_secret" "heroku_app_name" {
  repository      = github_repository.my_repo.name
  secret_name     = "HEROKU_APP_NAME"
  plaintext_value = heroku_app.my_app.name
}
resource "github_actions_secret" "heroku_token" {
  repository      = github_repository.my_repo.name
  secret_name     = "HEROKU_API_KEY"
  plaintext_value = var.heroku_api_key
}

# Heroku formation --------------------------------
resource "heroku_formation" "my_app_web" {
  app      = heroku_app.my_app.name
  type     = "web"
  quantity = 1
  size     = "Hobby"
}

# Namecheap and Cloudlare --------------------------------
provider "namecheap" {
  user_name = var.namecheap_api_user
  api_user  = var.namecheap_api_user
  api_key   = var.namecheap_api_key
}
resource "namecheap_domain_records" "my_dns" {
  domain = var.domain_name
  mode   = "OVERWRITE"

  nameservers = [
    "tia.ns.cloudflare.com",
    "wilson.ns.cloudflare.com"
  ]
}
provider "cloudflare" {
  email                = var.cloudflare_username
  api_key              = var.cloudflare_api_key
  api_user_service_key = var.cloudflare_ca_key
}
resource "cloudflare_zone" "my_zone" {
  zone = var.domain_name
  plan = "free"
}
resource "cloudflare_zone_settings_override" "my_zone_settings" {
  zone_id = cloudflare_zone.my_zone.id
  settings {
    always_use_https         = "on"
    tls_1_3                  = "on"
    automatic_https_rewrites = "on"
    ssl                      = "strict"
  }
}

# SSL --------------------------------
resource "tls_private_key" "private_key" {
  algorithm = "RSA"
}
resource "tls_cert_request" "cert_request" {
  key_algorithm   = tls_private_key.private_key.algorithm
  private_key_pem = tls_private_key.private_key.private_key_pem

  subject {
    common_name = var.domain_name
  }
}
resource "cloudflare_origin_ca_certificate" "my_ca_cert" {
  csr                = tls_cert_request.cert_request.cert_request_pem
  hostnames          = [var.domain_name, var.api_domain_name]
  request_type       = "origin-rsa"
  requested_validity = 365
}

# Api server Dns --------------------------------
resource "heroku_ssl" "my_ssl" {
  app_id            = heroku_app.my_app.uuid
  certificate_chain = cloudflare_origin_ca_certificate.my_ca_cert.certificate
  private_key       = tls_private_key.private_key.private_key_pem
  depends_on        = [heroku_formation.my_app_web]
}
resource "heroku_domain" "my_api_domain" {
  app             = heroku_app.my_app.name
  hostname        = var.api_domain_name
  sni_endpoint_id = heroku_ssl.my_ssl.id
}
resource "cloudflare_record" "my_cname" {
  zone_id = cloudflare_zone.my_zone.id
  name    = var.api_domain_name
  value   = heroku_domain.my_api_domain.cname
  type    = "CNAME"
  proxied = true
}
