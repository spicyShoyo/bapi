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
  }
}

# Import github repo and create heroku app --------------------------------
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
