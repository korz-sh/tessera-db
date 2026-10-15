resource "aws_s3_bucket" "data" {
  bucket_prefix = "${var.name}-data-"
  force_destroy = var.force_destroy_bucket
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_dynamodb_table" "coordination" {
  name         = "${var.name}-coordination"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "tenant_id"

  attribute {
    name = "tenant_id"
    type = "S"
  }

  attribute {
    name = "deletion_shard"
    type = "S"
  }

  attribute {
    name = "deletion_due"
    type = "S"
  }

  global_secondary_index {
    name            = "pending-deletions"
    hash_key        = "deletion_shard"
    range_key       = "deletion_due"
    projection_type = "KEYS_ONLY"
  }

  point_in_time_recovery {
    enabled = true
  }

  ttl {
    attribute_name = "quota_expires_at"
    enabled        = true
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}-sql"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/apigateway/${var.name}-sql"
  retention_in_days = 7
}

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name}-sql"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    sid       = "ListDataBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketVersioning"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid       = "UseDataObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }

  statement {
    sid = "CoordinatePublishedHeads"
    actions = [
      "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
      "dynamodb:DeleteItem", "dynamodb:ConditionCheckItem",
      "dynamodb:TransactWriteItems",
    ]
    resources = [aws_dynamodb_table.coordination.arn]
  }

  statement {
    sid       = "QueryPendingDeletions"
    actions   = ["dynamodb:Query"]
    resources = ["${aws_dynamodb_table.coordination.arn}/index/pending-deletions"]
  }

  statement {
    sid       = "ReadAdminSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.admin_secret_arn]
  }

  statement {
    sid       = "WriteLambdaLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.name}-sql"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_lambda_function" "sql" {
  function_name                  = "${var.name}-sql"
  filename                       = var.lambda_zip_path
  source_code_hash               = filebase64sha256(var.lambda_zip_path)
  role                           = aws_iam_role.lambda.arn
  handler                        = "tessera.experimental.cloud.lambda_handler.lambda_handler"
  runtime                        = "python3.12"
  architectures                  = ["x86_64"]
  memory_size                    = 512
  timeout                        = 30
  reserved_concurrent_executions = var.max_concurrency

  ephemeral_storage {
    size = var.ephemeral_storage_mb
  }

  environment {
    variables = {
      TESSERA_BUCKET                   = aws_s3_bucket.data.id
      TESSERA_TABLE                    = aws_dynamodb_table.coordination.name
      TESSERA_ADMIN_SECRET_ARN         = var.admin_secret_arn
      TESSERA_RETENTION_KEEP_LAST      = tostring(var.retention_keep_last)
      TESSERA_RETENTION_SECONDS        = tostring(var.retention_seconds)
      TESSERA_GC_GRACE_SECONDS         = tostring(var.gc_grace_seconds)
      TESSERA_NATIVE_GC_ENABLED        = var.enable_native_gc ? "1" : "0"
      TESSERA_RETENTION_METADATA_LIMIT = tostring(var.retention_metadata_limit)
      TESSERA_AWS_CALL_METRICS         = var.enable_aws_call_metrics ? "1" : "0"
      TESSERA_ALLOW_LEGACY_ROOT_CREATE = var.allow_legacy_root_create ? "1" : "0"
    }
  }

  depends_on = [aws_iam_role_policy.lambda, aws_cloudwatch_log_group.lambda]
}

resource "aws_apigatewayv2_api" "sql" {
  name          = "${var.name}-sql"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "sql" {
  api_id                 = aws_apigatewayv2_api.sql.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.sql.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "default" {
  api_id    = aws_apigatewayv2_api.sql.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.sql.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.sql.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api.arn
    format = jsonencode({
      request_id     = "$context.requestId"
      status         = "$context.status"
      response_bytes = "$context.responseLength"
    })
  }

  default_route_settings {
    throttling_burst_limit = 20
    throttling_rate_limit  = 10
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowApiGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sql.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.sql.execution_arn}/*/*"
}

resource "aws_cloudwatch_event_rule" "deletion_sweep" {
  name                = "${var.name}-deletion-sweep"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_rule" "deletion_repair" {
  name                = "${var.name}-deletion-repair"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "deletion_repair" {
  rule      = aws_cloudwatch_event_rule.deletion_repair.name
  target_id = "DeletionRepair"
  arn       = aws_lambda_function.sql.arn
  input     = jsonencode({ tessera_maintenance = "deletion_repair" })
}

resource "aws_lambda_permission" "deletion_repair" {
  statement_id  = "AllowDeletionRepair"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sql.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.deletion_repair.arn
}

resource "aws_cloudwatch_event_target" "deletion_sweep" {
  rule      = aws_cloudwatch_event_rule.deletion_sweep.name
  target_id = "DeletionSweep"
  arn       = aws_lambda_function.sql.arn
}

resource "aws_lambda_permission" "deletion_sweep" {
  statement_id  = "AllowDeletionSweep"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sql.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.deletion_sweep.arn
}

resource "aws_cloudwatch_event_rule" "retention_sweep" {
  name                = "${var.name}-retention-sweep"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "retention_sweep" {
  rule      = aws_cloudwatch_event_rule.retention_sweep.name
  target_id = "RetentionSweep"
  arn       = aws_lambda_function.sql.arn
  input     = jsonencode({ tessera_maintenance = "retention" })
}

resource "aws_lambda_permission" "retention_sweep" {
  statement_id  = "AllowRetentionSweep"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sql.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.retention_sweep.arn
}
