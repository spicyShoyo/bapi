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
variable "namecheap_api_key" {
  sensitive = true
}
variable "namecheap_api_user" {
  sensitive = true
}
variable "cloudflare_api_key" {
  sensitive = true
}
variable "cloudflare_ca_key" {
  sensitive = true
}
variable "cloudflare_username" {
  sensitive = true
}
