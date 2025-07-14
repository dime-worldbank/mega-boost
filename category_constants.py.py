# Databricks notebook source
from enum import Enum

class EconCategory(Enum):
    WAGE_BILL = "Wage bill"
    CAPITAL_EXPENDITURES = "Capital expenditures"
    GOODS_AND_SERVICES = "Goods and services"
    SUBSIDIES = "Subsidies"
    SOCIAL_BENEFITS = "Social benefits"
    INTEREST_ON_DEBT = "Interest on debt"
    OTHER_EXPENSES = "Other expenses"
    RECURRENT_MAINTENANCE = "Recurrent Maintenance"
    SUBSIDIES_TO_PRODUCTION = "Subsidies to Production"
    PENSIONS = "Pensions"
    SOCIAL_ASSISTANCE = "Social Assistance"
    BASIC_WAGES = "Basic Wages"
    CAPITAL_MAINTENANCE = "Capital Maintenance"
    # Add more as needed

class EconSubCategory(Enum):
    PENSIONS = "Pensions"
    SOCIAL_ASSISTANCE = "Social Assistance"
    BASIC_WAGES = "Basic Wages"
    CAPITAL_MAINTENANCE = "Capital Maintenance"
    RECURRENT_MAINTENANCE = "Recurrent Maintenance"
    SUBSIDIES_TO_PRODUCTION = "Subsidies to Production"
    OTHER_EXPENSES = "Other expenses"

class FuncCategory(Enum):
    HOUSING_AND_COMMUNITY_AMENITIES = "Housing and community amenities"
    DEFENCE = "Defence"
    PUBLIC_ORDER_AND_SAFETY = "Public order and safety"
    ENVIRONMENTAL_PROTECTION = "Environmental protection"
    HEALTH = "Health"
    SOCIAL_PROTECTION = "Social protection"
    EDUCATION = "Education"
    RECREATION_CULTURE_AND_RELIGION = "Recreation culture and religion"
    ECONOMIC_AFFAIRS = "Economic affairs"
    GENERAL_PUBLIC_SERVICES = "General public services"

class FuncSubCategory(Enum):
    PUBLIC_SAFETY = "Public Safety"
    JUDICIARY = "Judiciary"
    TERTIARY_EDUCATION = "Tertiary Education"
    AGRICULTURE = "Agriculture"
    TELECOM = "Telecom"
    TRANSPORT = "Transport"
    OTHER_EXPENSES = "Other expenses"

# Global category lists for all countries

econ_categories = [cat.value for cat in EconCategory]
econsub_categories = [cat.value for cat in EconSubCategory]
func_categories = [cat.value for cat in FuncCategory]
funcsub_categories = [cat.value for cat in FuncSubCategory]

