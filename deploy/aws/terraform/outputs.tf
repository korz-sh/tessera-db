output "api_url" {
  description = "HTTPS endpoint for the experimental SQL API."
  value       = aws_apigatewayv2_api.sql.api_endpoint
}

output "data_bucket" {
  value = aws_s3_bucket.data.id
}

output "coordination_table" {
  value = aws_dynamodb_table.coordination.name
}

output "lambda_function" {
  value = aws_lambda_function.sql.function_name
}
