# Databricks notebook source
CODE_LABEL_MAPPING = {
    "EXP_ECON_TOT_EXP_EXE": {
        "category": " Spending: Total Expenditures ",
        "parent": "Total Expenditures",
        "child": "",
        "parent_type": "total",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_TOT_EXP_FOR_EXE": {
        "category": "Spending: Total Expenditures (foreign funded)",
        "parent": "Total Expenditures(Foreign Funded)",
        "child": "",
        "parent_type": "total",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_WAG_BIL_EXE": {
        "category": "Spending: Wage bill",
        "parent": "Wage bill",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_BAS_WAG_EXE": {
        "category": " Spending in basic wages ",
        "parent": "Wage bill",
        "child": "Basic Wages",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_ALL_EXE": {
        "category": " Spending in allowances ",
        "parent": "Wage bill",
        "child": "Allowances",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_PEN_CON_EXE": {
        "category": " Social benefits - (pension contributions) ",
        "parent": "Wage bill",
        "child": "Social Benefits (pension contributions)",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_CAP_EXP_EXE": {
        "category": "Spending: Capital Expenditures",
        "parent": "Capital expenditures",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_CAP_EXP_FOR_EXE": {
        "category": " Spending: Capital Expenditures (foreign spending) ",
        "parent": "Capital expenditures",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_CAP_MAI_EXE": {
        "category": " Spending in capital maintenance ",
        "parent": "Capital expenditures",
        "child": "Capital Maintenance",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_USE_GOO_SER_EXE": {
        "category": "Spending: Use of Goods and services",
        "parent": "Goods and services",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_GOO_SER_BAS_SER_EXE": {
        "category": " Spending in Goods and services (basic services) ",
        "parent": "Goods and services",
        "child": "Basic Services",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_GOO_SER_EMP_CON_EXE": {
        "category": " Spending in Goods and services (employment contracts) ",
        "parent": "Goods and services",
        "child": "Employment Contracts",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_REC_MAI_EXE": {
        "category": " Spending in recurrent maintenance ",
        "parent": "Goods and services",
        "child": "Recurrent Maintenance",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_SUB_EXE": {
        "category": "Spending in subsidies",
        "parent": "Subsidies",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SUB_PRO_EXE": {
        "category": " Spending: Subsidies to production ",
        "parent": "Subsidies",
        "child": "Subsidies to Production",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_SOC_BEN_EXE": {
        "category": "Social benefits",
        "parent": "Social benefits",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SOC_ASS_EXE": {
        "category": " Social Assistance ",
        "parent": "Social benefits",
        "child": "Social Assistance",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_SOC_BEN_PEN_EXE": {
        "category": " Pensions ",
        "parent": "Social benefits",
        "child": "Pensions",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_OTH_SOC_BEN_EXE": {
        "category": " Other social benefits ",
        "parent": "Social benefits",
        "child": "Other Social Benefits",
        "parent_type": "econ",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_ECON_OTH_GRA_EXE": {
        "category": "Other grants/transfers",
        "parent": "Other grants and transfers",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_OTH_EXP_EXE": {
        "category": "Other expenses",
        "parent": "Other expenses",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "": {
        "category": "",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_GEN_PUB_SER_EXE": {
        "category": "General public services (COFOG 701)",
        "parent": "General public services",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_DEB_REP_EXE": {
        "category": "Spending: Debt repayment",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_INT_DEB_EXE": {
        "category": "Spending: Interest on debt",
        "parent": "Interest on debt",
        "child": "Subtotal",
        "parent_type": "econ",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_DEF_EXE": {
        "category": "Defense (COFOG 702)",
        "parent": "Defence",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_PUB_ORD_SAF_EXE": {
        "category": "Public order and safety (COFOG 703)",
        "parent": "Public order and safety",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_JUD_EXE": {
        "category": " Spending in judiciary ",
        "parent": "Judiciary",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_JUD_EXE": {
        "category": " Spending in wages in judiciary ",
        "parent": "Judiciary",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_BAS_WAG_JUD_EXE": {
        "category": " Spending in basic wages in judiciary ",
        "parent": "Judiciary",
        "child": "Basic Wages",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_ALL_JUD_EXE": {
        "category": " Spending in allowances in judiciary ",
        "parent": "Judiciary",
        "child": "Allowances",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_FUNC_PUB_SAF_EXE": {
        "category": " Spending in public safety ",
        "parent": "Public Safety",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_PUB_SAF_EXE": {
        "category": " Spending in wages in public safety ",
        "parent": "Public Safety",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_BAS_WAG_PUB_SAF_EXE": {
        "category": " Spending in basic wages in public safety ",
        "parent": "Public Safety",
        "child": "Basic Wages",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_ALL_PUB_SAF_EXE": {
        "category": " Spending in allowances in public safety ",
        "parent": "Public Safety",
        "child": "Allowances",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_FUNC_ECO_REL_EXE": {
        "category": "Economic relations (COFOG 704)",
        "parent": "Economic affairs",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_AGR_EXE": {
        "category": " Spending in agriculture ",
        "parent": "Agriculture",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_AGR_EXE": {
        "category": " Spending in wages in agriculture ",
        "parent": "Agriculture",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_BAS_WAG_AGR_EXE": {
        "category": " Spending in basic wages in agriculture ",
        "parent": "Agriculture",
        "child": "Basic Wages",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_ALL_AGR_EXE": {
        "category": " Spending in allowances in agriculture ",
        "parent": "Agriculture",
        "child": "Allowances",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_AGR_EXE": {
        "category": " Spending: Capital expenditures in agriculture ",
        "parent": "Agriculture",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_AGR_FOR_EXE": {
        "category": " Spending in agriculture (foreign) ",
        "parent": "Agriculture",
        "child": "Total Expenditures(Foreign Funded)",
        "parent_type": "func_sub",
        "child_type": "total",
        "subnational": False
    },
    "EXP_CROSS_SUB_AGR_EXE": {
        "category": "Spending in subsidies in agriculture",
        "parent": "Agriculture",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_AGR_EXE": {
        "category": "Spending in recurrent mainternance in agriculture",
        "parent": "Agriculture",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_FUNC_IRR_EXE": {
        "category": " Spending in Irrigation ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_AGR_FOR_EXE": {
        "category": "Spending: Capital expenditures in agriculture (foreign)",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_IRR_EXE": {
        "category": "Spending: Capital expenditures in irrigation",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_IRR_FOR_EXE": {
        "category": "Spending: Capital expenditures in irrigation (foreign funded)",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_IRR_DOM_EXE": {
        "category": "Spending: Capital expenditures in irrigation (domestic funded)",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IRR_EXE": {
        "category": "Spending in subsidies in irrigation",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_IRR_EXE": {
        "category": "Spending in wages in irrigation",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_IRR_EXE": {
        "category": "Spending in capital maintenance in irrigation",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_IRR_EXE": {
        "category": "Spending in recurrent maintenance in irrigation",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_IRR_FOR_EXE": {
        "category": "Spending in irrigation (foreign)",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_TRA_EXE": {
        "category": " Spending in transport ",
        "parent": "Transport",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_TRA_FOR_EXE": {
        "category": " Spending in transport (foreign) ",
        "parent": "Transport",
        "child": "Total Expenditures(Foreign Funded)",
        "parent_type": "func_sub",
        "child_type": "total",
        "subnational": False
    },
    "EXP_CROSS_WAG_TRA_EXE": {
        "category": "  Spending in wages in transport ",
        "parent": "Transport",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_TRA_EXE": {
        "category": " Spending: Capital expenditures in transport ",
        "parent": "Transport",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_TRA_FOR_EXE": {
        "category": " Spending: Capital expenditures in transport (foreign) ",
        "parent": "Transport",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_TRA_EXE": {
        "category": " Capital Transfers to SOEs in transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_TRA_EXE": {
        "category": " Current Transfers to SOEs in transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_TRA_EXE": {
        "category": " Spending in recurrent maintenance in transport ",
        "parent": "Transport",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_TRA_EXE": {
        "category": " Spending in capital maintenance in transport ",
        "parent": "Transport",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_TRA_EXE": {
        "category": " Spending in subsidies in transport ",
        "parent": "Transport",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_TRA_EXE": {
        "category": " Spending in subsidies to firms in transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_TRA_EXE": {
        "category": " Spending in subsidies to individuals in transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ROA_EXE": {
        "category": " Spending in roads ",
        "parent": "Roads",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_ROA_EXE": {
        "category": " Spending in wages in roads ",
        "parent": "Roads",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_BAS_WAG_ROA_EXE": {
        "category": " Spending in basic wages in roads ",
        "parent": "Roads",
        "child": "Basic Wages",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_ALL_ROA_EXE": {
        "category": " Spending in allowances in roads ",
        "parent": "Roads",
        "child": "Allowances",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ROA_EXE": {
        "category": " Spending: Capital expenditures in roads ",
        "parent": "Roads",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ROA_FOR_EXE": {
        "category": " Spending: Capital expenditures in roads (foreign funded) ",
        "parent": "Roads",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ROA_DOM_EXE": {
        "category": " Spending: Capital expenditures in roads (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_ROA_EXE": {
        "category": " Capital Transfers to SOEs in roads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_ROA_EXE": {
        "category": " Current Transfers to SOEs in roads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_ROA_EXE": {
        "category": " Spending in recurrent maintenance in roads ",
        "parent": "Roads",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_ROA_EXE": {
        "category": " Spending in capital maintenance in roads ",
        "parent": "Roads",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_ROA_EXE": {
        "category": " Spending in subsidies in roads ",
        "parent": "Roads",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_ROA_EXE": {
        "category": " Spending in subsidies to firms in roads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_ROA_EXE": {
        "category": " Spending in subsidies to individuals in roads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_RAI_EXE": {
        "category": " Spending in railroads ",
        "parent": "Railroads",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_RAI_EXE": {
        "category": " Spending in wages in railroad ",
        "parent": "Railroads",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_RAI_EXE": {
        "category": " Spending: Capital expenditures in railroad ",
        "parent": "Railroads",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_RAI_FOR_EXE": {
        "category": " Spending: Capital expenditures in railroad (foreign funded) ",
        "parent": "Railroads",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_RAI_DOM_EXE": {
        "category": " Spending: Capital expenditures in railroad (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_RAI_EXE": {
        "category": " Capital Transfers to SOEs in railroads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_RAI_EXE": {
        "category": " Current Transfers to SOEs in railroads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_RAI_EXE": {
        "category": " Spending in recurrent maintenance in railroads ",
        "parent": "Railroads",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_RAI_EXE": {
        "category": " Spending in capital maintenance in railroads ",
        "parent": "Railroads",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_RAI_EXE": {
        "category": " Spending in subsidies in railroads ",
        "parent": "Railroads",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_RAI_EXE": {
        "category": " Spending in subsidies to firms in railroads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_RAI_EXE": {
        "category": " Spending in subsidies to individuals in railroads ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_WAT_TRA_EXE": {
        "category": " Spending in water transport ",
        "parent": "Water Transport",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_WAT_TRA_EXE": {
        "category": " Spending in wages in water transport ",
        "parent": "Water Transport",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_WAT_TRA_EXE": {
        "category": " Spending: Capital expenditures in water transport ",
        "parent": "Water Transport",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_WAT_TRA_FOR_EXE": {
        "category": " Spending: Capital expenditures in water transport (foreign funded) ",
        "parent": "Water Transport",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_WAT_TRA_DOM_EXE": {
        "category": " Spending: Capital expenditures in water transport (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_WAT_TRA_EXE": {
        "category": " Capital Transfers to SOEs in water transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_WAT_TRA_EXE": {
        "category": " Current Transfers to SOEs in water transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_WAT_TRA_EXE": {
        "category": " Spending in recurrent maintenance in water transport ",
        "parent": "Water Transport",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_WAT_TRA_EXE": {
        "category": " Spending in capital maintenance in water transport ",
        "parent": "Water Transport",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_WAT_TRA_EXE": {
        "category": " Spending in subsidies in water transport ",
        "parent": "Water Transport",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_WAT_TRA_EXE": {
        "category": " Spending in subsidies to firms in water transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_WAT_TRA_EXE": {
        "category": " Spending in subsidies to individuals in water transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_AIR_TRA_EXE": {
        "category": " Spending in air transport ",
        "parent": "Air Transport",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_AIR_TRA_EXE": {
        "category": " Spending in wages in air transport ",
        "parent": "Air Transport",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_AIR_TRA_EXE": {
        "category": " Spending: Capital expenditures in air transport ",
        "parent": "Air Transport",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_AIR_TRA_FOR_EXE": {
        "category": " Spending: Capital expenditures in air transport (foreign funded) ",
        "parent": "Air Transport",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_AIR_TRA_DOM_EXE": {
        "category": " Spending: Capital expenditures in air transport (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_AIR_TRA_EXE": {
        "category": " Capital Transfers to SOEs in air transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_AIR_TRA_EXE": {
        "category": " Current Transfers to SOEs in air transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_AIR_TRA_EXE": {
        "category": " Spending in recurrent maintenance in air transport ",
        "parent": "Air Transport",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_AIR_TRA_EXE": {
        "category": " Spending in capital maintenance in air transport ",
        "parent": "Air Transport",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_AIR_TRA_EXE": {
        "category": " Spending in subsidies in air transport ",
        "parent": "Air Transport",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_AIR_TRA_EXE": {
        "category": " Spending in subsidies to firms in air transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_AIR_TRA_EXE": {
        "category": " Spending in subsidies to individuals in air transport ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENE_EXE": {
        "category": " Spending in energy ",
        "parent": "Energy",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENE_FOR_EXE": {
        "category": " Spending in energy (foreign) ",
        "parent": "Energy",
        "child": "Total Expenditures(Foreign Funded)",
        "parent_type": "func_sub",
        "child_type": "total",
        "subnational": False
    },
    "EXP_CROSS_WAG_ENE_EXE": {
        "category": " Spending in wages in energy ",
        "parent": "Energy",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_EXE": {
        "category": " Spending: Capital expenditures in energy ",
        "parent": "Energy",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_FOR_EXE": {
        "category": " Spending: Capital expenditures in energy (foreign funded) ",
        "parent": "Energy",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_DOM_EXE": {
        "category": " Spending: Capital expenditures in energy (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_ENE_EXE": {
        "category": " Capital Transfers to SOEs in energy ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_ENE_EXE": {
        "category": " Current Transfers to SOEs in energy ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_ENE_EXE": {
        "category": " Spending in recurrent maintenance in energy ",
        "parent": "Energy",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_ENE_EXE": {
        "category": " Spending in capital maintenance in energy ",
        "parent": "Energy",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_ENE_EXE": {
        "category": " Spending in subsidies in energy ",
        "parent": "Energy",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_ENE_EXE": {
        "category": " Spending in subsidies to firms in energy ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_ENE_EXE": {
        "category": " Spending in subsidies to individuals in energy ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENE_POW_EXE": {
        "category": " Spending in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_ENE_POW_EXE": {
        "category": " Spending in wages in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_POW_EXE": {
        "category": " Spending: Capital expenditures in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_POW_FOR_EXE": {
        "category": " Spending: Capital expenditures in energy (power) (foreign funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_POW_DOM_EXE": {
        "category": " Spending: Capital expenditures in energy (power) (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_ENE_POW_EXE": {
        "category": " Capital Transfers to SOEs in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_ENE_POW_EXE": {
        "category": " Current Transfers to SOEs in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_ENE_POW_EXE": {
        "category": " Spending in recurrent maintenance in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_ENE_POW_EXE": {
        "category": " Spending in capital maintenance in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_ENE_POW_EXE": {
        "category": " Spending in subsidies in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_ENE_POW_EXE": {
        "category": " Spending in subsidies to firms in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_ENE_POW_EXE": {
        "category": " Spending in subsidies to individuals in energy (power) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENE_OIL_EXE": {
        "category": " Spending in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_ENE_OIL_GAS_EXE": {
        "category": " Spending in wages in energy (oil/gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_OIL_GAS_EXE": {
        "category": " Spending: Capital expenditures in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_OIL_GAS_FOR_EXE": {
        "category": " Spending: Capital expenditures in energy (oil & gas) (foreign funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_OIL_GAS_DOM_EXE": {
        "category": " Spending: Capital expenditures in energy (oil & gas) (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_ENE_OIL_GAS_EXE": {
        "category": " Capital Transfers to SOEs in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_ENE_OIL_GAS_EXE": {
        "category": " Current Transfers to SOEs in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_ENE_OIL_GAS_EXE": {
        "category": " Spending in recurrent maintenance in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_ENE_OIL_GAS_EXE": {
        "category": " Spending in capital maintenance in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_ENE_OIL_GAS_EXE": {
        "category": " Spending in subsidies in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_ENE_OIL_GAS_EXE": {
        "category": " Spending in subsidies to firms in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_ENE_OIL_GAS_EXE": {
        "category": " Spending in subsidies to individuals in energy (oil & gas) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENE_HEA_EXE": {
        "category": " Spending in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_HEA_EXE": {
        "category": " Spending: Capital expenditures in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_HEA_FOR_EXE": {
        "category": " Spending: Capital expenditures in energy (heating) (foreign funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_ENE_HEA_DOM_EXE": {
        "category": " Spending: Capital expenditures in energy (heating) (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_TRA_SOE_ENE_HEA_EXE": {
        "category": " Capital Transfers to SOEs in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CUR_TRA_SOE_ENE_HEA_EXE": {
        "category": " Current Transfers to SOEs in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_ENE_HEA_EXE": {
        "category": " Spending in recurrent maintenance in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_ENE_HEA_EXE": {
        "category": " Spending in capital maintenance in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_ENE_HEA_EXE": {
        "category": " Spending in subsidies in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_FIR_ENE_HEA_EXE": {
        "category": " Spending in subsidies to firms in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_IND_ENE_HEA_EXE": {
        "category": " Spending in subsidies to individuals in energy (heating) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_HYD_EXE": {
        "category": "Spending in hydropower",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_HYD_EXE": {
        "category": " Spending: Capital expenditures in hydropower ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_HYD_FOR_EXE": {
        "category": " Spending: Capital expenditures in hydropower (foreign funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_HYD_DOM_EXE": {
        "category": " Spending: Capital expenditures in hydropower (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_HYD_EXE": {
        "category": " Spending in subsidies in hydropower ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_HYD_EXE": {
        "category": " Spending in wages in hydropower ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_HYD_EXE": {
        "category": " Spending in capital maintenance in hydropower ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_HYD_EXE": {
        "category": " Spending in recurrent maintenance in hydropower ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_HYD_FOR_EXE": {
        "category": "Spending in hydropower (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_TEL_EXE": {
        "category": " Spending in telecoms ",
        "parent": "Economic affairs",
        "child": "Telecom",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_TEL_EXE": {
        "category": " Spending: Capital expenditures in telecoms ",
        "parent": "Economic affairs",
        "child": "Telecom",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_TEL_FOR_EXE": {
        "category": " Spending: Capital expenditures in telecoms (foreign funded) ",
        "parent": "Economic affairs",
        "child": "Telecom",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_TEL_DOM_EXE": {
        "category": " Spending: Capital expenditures in telecoms (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENV_PRO_EXE": {
        "category": "Environment Protection (COFOG 705)",
        "parent": "Environmental protection",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_WAS_MAN_EXE": {
        "category": " Waste Management ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_WAS_WAT_MAN_EXE": {
        "category": " Waste Water Management ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_POL_ABA_EXE": {
        "category": " Pollution Abatement ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_PRO_BIO_LAN_EXE": {
        "category": " Protection Of Biodiversity And Landscape ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_R&D_ENV_PRO_EXE": {
        "category": " R&D Environmental Protection ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_ENV_PRO_NEC_EXE": {
        "category": " Environmental Protection N.E.C ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_FLO_PRO_EXE": {
        "category": " Flood protection spending ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_OTH_CLI_RES_EXE": {
        "category": " Other Climate resilience expenditures ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_HOU_EXE": {
        "category": "Housing (COFOG 706)",
        "parent": "Housing and community amenities",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_WAT_SAN_EXE": {
        "category": " Spending in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_WAT_SAN_EXE": {
        "category": " Spending: Capital expenditures in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_WAT_SAN_FOR_EXE": {
        "category": " Spending: Capital expenditures in water and sanitation (foreign funded) ",
        "parent": "Water and Sanitation",
        "child": "Capital Expenditure (foreign spending)",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_WAT_SAN_DOM_EXE": {
        "category": " Spending: Capital expenditures in water and sanitation (domestic funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SUB_WAT_SAN_EXE": {
        "category": " Spending in subsidies in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Subsidies",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_WAG_WAT_SAN_EXE": {
        "category": " Spending in wages in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_CAP_MAI_WAT_SAN_EXE": {
        "category": " Spending in capital maintenance in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Capital Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_WAT_SAN_EXE": {
        "category": " Spending in recurrent maintenance in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_FUNC_WAT_SAN_FOR_EXE": {
        "category": " Spending in water and sanitation (foreign) ",
        "parent": "Water and Sanitation",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_HEA_EXE": {
        "category": " Health (COFOG 707) ",
        "parent": "Health",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_HEA_EXE": {
        "category": " Spending in wages in health ",
        "parent": "Health",
        "child": "Wage bill",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_BAS_WAG_HEA_EXE": {
        "category": " Spending in basic wages in health ",
        "parent": "Health",
        "child": "Basic Wages",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_ALL_HEA_EXE": {
        "category": " Spending in allowances in health ",
        "parent": "Health",
        "child": "Allowances",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_HEA_EXE": {
        "category": " Spending: Capital expenditures in health ",
        "parent": "Health",
        "child": "Capital expenditures",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_HEA_EXE": {
        "category": " Spending in Goods and services in health ",
        "parent": "Health",
        "child": "Goods and services",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_IMM_EXE": {
        "category": " Spending in immunization ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_IMM_FOR_EXE": {
        "category": " Spending in immunization (foreign funded) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_HEA_FOR_EXE": {
        "category": " Spending in health (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_HEA_FOR_EXE": {
        "category": " Spending: Capital expenditures in health (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_HEA_FOR_EXE": {
        "category": " Spending in Goods and services in health (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_PRI_HEA_EXE": {
        "category": " Spending in primary/secondary health ",
        "parent": "Health",
        "child": "Primary and Secondary Health",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_FUNC_TER_HEA_EXE": {
        "category": " Spending in tertiary/quaternary health ",
        "parent": "Health",
        "child": "Tertiary and Quaternary Health",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_EMP_CON_HEA_EXE": {
        "category": " Spending in Goods and services (employment contracts) in health ",
        "parent": "Health",
        "child": "Employment Contracts",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_BAS_SER_HEA_EXE": {
        "category": " Spending in Goods and services (basic services) in health ",
        "parent": "Health",
        "child": "Basic Services",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_HEA_EXE": {
        "category": "Spending in subsidies in health",
        "parent": "Health",
        "child": "Subsidies",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_HEA_EXE": {
        "category": "Spending in recurrent mainternance in health",
        "parent": "Health",
        "child": "Recurrent Maintenance",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_FUNC_REV_CUS_EXC_EXE": {
        "category": " Recreation, culture and religion (COFOG 708) ",
        "parent": "Recreation, culture and religion",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_EDU_EXE": {
        "category": " Education (COFOG 709) ",
        "parent": "Education",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_EDU_EXE": {
        "category": " Spending in wages in education ",
        "parent": "Education",
        "child": "Wage bill",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_BAS_WAG_EDU_EXE": {
        "category": " Spending in basic wages in education ",
        "parent": "Education",
        "child": "Basic Wages",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_ALL_EDU_EXE": {
        "category": " Spending in allowances in education ",
        "parent": "Education",
        "child": "Allowances",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_EDU_EXE": {
        "category": " Spending: Capital expenditures in education ",
        "parent": "Education",
        "child": "Capital expenditures",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_EDU_EXE": {
        "category": " Spending in Goods and services in education ",
        "parent": "Education",
        "child": "Goods and services",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_PRI_EDU_EXE": {
        "category": " Spending in primary education ",
        "parent": "Education",
        "child": "Primary Education",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_WAG_PRI_EDU_EXE": {
        "category": " Spending in wages in primary education ",
        "parent": "Primary Education",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_SEC_EDU_EXE": {
        "category": " Spending in secondary education ",
        "parent": "Education",
        "child": "Secondary Education",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_WAG_SEC_EDU_EXE": {
        "category": " Spending in wages in secondary education ",
        "parent": "Secondary Education",
        "child": "Wage bill",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_PRI_SEC_EDU_EXE": {
        "category": " Spending in primary and secondary education ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_WAG_PRI_SEC_EDU_EXE": {
        "category": " Spending in wages in primary and secondary education ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_TER_EDU_EXE": {
        "category": " Spending in tertiary education ",
        "parent": "Education",
        "child": "Tertiary Education",
        "parent_type": "func",
        "child_type": "func_sub",
        "subnational": False
    },
    "EXP_CROSS_WAG_TER_EDU_EXE": {
        "category": " Spending in wages in tertiary education ",
        "parent": "Tertiary Education",
        "child": "Basic Wages",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_EDU_FOR_EXE": {
        "category": " Spending in education (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_CAP_EXP_EDU_FOR_EXE": {
        "category": " Spending: Capital expenditures in education (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_EDU_FOR_EXE": {
        "category": " Spending in Goods and services in education (foreign) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_EMP_CON_EDU_EXE": {
        "category": "Spending in Goods and services (employment contracts) in education",
        "parent": "Education",
        "child": "Employment Contracts",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_GOO_SER_BAS_SER_EDU_EXE": {
        "category": "Spending in Goods and services (basic services) in education",
        "parent": "Education",
        "child": "Basic Services",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SUB_EDU_EXE": {
        "category": "Spending in subsidies in education",
        "parent": "Education",
        "child": "Subsidies",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_REC_MAI_EDU_EXE": {
        "category": "Spending in recurrent mainternance in education",
        "parent": "Education",
        "child": "Recurrent Maintenance",
        "parent_type": "func",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_FUNC_SOC_PRO_EXE": {
        "category": " Social Protection (COFOG 710) ",
        "parent": "Social protection",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SBN_TOT_SPE_EXE": {
        "category": " Subnational: Total spending ",
        "parent": "Subnational Total",
        "child": "Subtotal",
        "parent_type": "total",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SBN_TOT_TRA_EXE": {
        "category": "Subnational: Total transfers",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_TRA_EDU_EXE": {
        "category": " Subnational: transfers in education ",
        "parent": "Education",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_TRA_HEA_EXE": {
        "category": " Subnational: transfers in health ",
        "parent": "Health",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_TRA_AGR_EXE": {
        "category": "Subnational: Total transfers",
        "parent": "Agriculture",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_TRA_ROA_EXE": {
        "category": " Subnational: transfers in roads ",
        "parent": "Roads",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_TRA_ENE_EXE": {
        "category": " Subnational: transfers in energy ",
        "parent": "Energy",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_TRA_WAT_SAN_EXE": {
        "category": " Subnational: transfers in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SBN_EAR_TRA_EXE": {
        "category": " Subnational: Earmarked transfers ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SBN_DIS_TRA_EXE": {
        "category": " Subnational: Discretionary transfers ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SBN_REC_SPE_EXE": {
        "category": " Subnational: Recurrent spending ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_SBN_CAP_SPE_EXE": {
        "category": " Subnational: Capital spending ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_SBN_EDU_EXE": {
        "category": " Subnational: Spending in education ",
        "parent": "Education",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_CAP_EDU_EXE": {
        "category": " Subnational: Capital Spending in education ",
        "parent": "Education",
        "child": "Capital expenditures",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_SBN_HEA_EXE": {
        "category": " Subnational: Spending in health ",
        "parent": "Health",
        "child": "Subtotal",
        "parent_type": "func",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_CAP_HEA_EXE": {
        "category": " Subnational: Capital Spending in health ",
        "parent": "Health",
        "child": "Capital expenditures",
        "parent_type": "func",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_SBN_AGR_EXE": {
        "category": " Subnational: Spending in agriculture ",
        "parent": "Agriculture",
        "child": "Subtotal",
        "parent_type": "func_sub",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_CAP_EXP_ROA_EXE": {
        "category": " Subnational: Capital expenditures in roads ",
        "parent": "Roads",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SBN_CAP_EXP_ENE_EXE": {
        "category": " Subnational: Capital expenditures in energy ",
        "parent": "Energy",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE": {
        "category": " Subnational: Capital expenditures in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_CROSS_SBN_REC_EXP_ROA_EXE": {
        "category": " Subnational: Recurrent expenditures in roads ",
        "parent": "Roads",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SBN_REC_EXP_ENE_EXE": {
        "category": " Subnational: Recurrent expenditures in energy ",
        "parent": "Energy",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SBN_REC_EXP_WAT_SAN_EXE": {
        "category": " Subnational: Recurrent expenditures in water and sanitation ",
        "parent": "Water and Sanitation",
        "child": "Recurrent Maintenance",
        "parent_type": "func_sub",
        "child_type": "econ_sub",
        "subnational": False
    },
    "EXP_CROSS_SBN_CAP_AGR_EXE": {
        "category": " Subnational: Capital Spending in agriculture ",
        "parent": "Agriculture",
        "child": "Capital expenditures",
        "parent_type": "func_sub",
        "child_type": "econ",
        "subnational": False
    },
    "EXP_FUNC_SBN_IRR_EXE": {
        "category": " Subnational: Spending in irrigation ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_FUNC_SBN_HYD_EXE": {
        "category": " Subnational: Spending in hydropower ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_IRR_REC_EXE": {
        "category": " Subnational: Spending in irrigation (recurrent) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_HYD_REC_EXE": {
        "category": " Subnational: Spending in hydropower (recurrent) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_IRR_CAP_EXE": {
        "category": " Subnational: Spending in irrigation (capital) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_CROSS_SBN_HYD_CAP_EXE": {
        "category": " Subnational: Spending in hydropower (capital) ",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_OTH_MED_RIG_EXP_EXE": {
        "category": "Other medium rigidity expenditures",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_OTH_HIG_RIG_EXE": {
        "category": "Other high rigid expenditures",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_COR_LOW_RIG_EXE": {
        "category": "Correction - low rigidity",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_COR_MED_RIG_EXE": {
        "category": "Correction - medium rigidity",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_COR_HIG_RIG_EXE": {
        "category": "Correction - high rigidity",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_HIG_RIG_EXE": {
        "category": "High Rigidity spending",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_MED_RIG_EXE": {
        "category": "Medium Rigidity spending",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    },
    "EXP_ECON_LOW_RIG_EXE": {
        "category": "Low Rigidity spending",
        "parent": "",
        "child": "",
        "parent_type": "",
        "child_type": "",
        "subnational": False
    }
}
