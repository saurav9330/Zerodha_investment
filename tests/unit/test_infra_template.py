"""Tests for infrastructure configuration choices."""

from pathlib import Path


def test_cloudformation_uses_glue_etl_and_redshift_serverless() -> None:
    template_text = Path("infra/cloudformation/template.yaml").read_text(encoding="utf-8")

    assert "AWS::RedshiftServerless::Namespace" in template_text
    assert "AWS::RedshiftServerless::Workgroup" in template_text
    assert "Name: glueetl" in template_text
    assert "ScheduleExpressionTimezone: Asia/Kolkata" in template_text
    assert "redshift-data:BatchExecuteStatement" in template_text
