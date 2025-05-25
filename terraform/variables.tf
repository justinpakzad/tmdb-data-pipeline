variable "region" {
  default = "us-east-1"
  type = string
}

variable "bucket" {
  default = "tmdb-data-jp"
  type = string
}

variable "max_capacity" {
  default = 0.0625
  type = number
}




variable "tmdb_api_key" {
  type  = string
  description = "TMDb API key"
}

variable "image_uri" {
  type = string
  default = "982081066484.dkr.ecr.us-east-1.amazonaws.com/tmdb-images:latest"
}