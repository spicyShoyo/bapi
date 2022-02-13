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
  size     = "Free"
}

# Dns --------------------------------
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

