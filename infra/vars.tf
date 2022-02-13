variable "repo_name" {
  type     = string
  nullable = false
}
variable "app_name" {
  type     = string
  nullable = false
}


variable "github_token" {
  sensitive = true
}
variable "heroku_api_key" {
  sensitive = true
}
variable "heroku_username" {
  sensitive = true
}
