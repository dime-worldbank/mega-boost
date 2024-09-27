-- Databricks notebook source
ALTER TABLE
  prd_mega.boost.boost_gold
SET
  TAGS (
    'name' = 'BOOST Harmonized',
    'subject' = 'Finance',
    'classification' = 'Official Use Only',
    'category' = 'Public Sector',
    'subcategory' = 'Financial Management',
    'frequency' = 'Annually',
    'collection' = 'Financial Management (FM)',
    'source' = 'BOOST',
    'domain' = 'Budget',
    'subdomain' = 'Budget & Cost Accounting'
  )
