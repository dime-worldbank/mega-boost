-- Databricks notebook source
CREATE
OR REFRESH LIVE TABLE data_availability USING DELTA AS (
  WITH time_coverage AS (
    SELECT
      country_name,
      min(year) as boost_earliest_year,
      max(year) as boost_latest_year
    FROM
      prd_mega.boost_intermediate.quality_total_gold
    WHERE
      executed is not NULL
    GROUP by
      1
  ),
  mega_coverage AS (
    SELECT
      DISTINCT country_name
    FROM
      prd_mega.boost.expenditure_by_country_year
  ),
  func_coverage AS (
    SELECT
      country_name,
      count(distinct func) as boost_num_func_cofog
    FROM
      prd_mega.boost_intermediate.quality_functional_gold
    WHERE
      executed is not NULL
    GROUP BY
      1
  ),
  pefa2016 as (
    SELECT
      country_name,
      concat_ws(', ', sort_array(collect_list(Year))) as pefa2016_years
    FROM
      prd_mega.indicator_intermediate.pefa_2016_silver
    GROUP BY
      1
  ),
  pefa2011 as (
    SELECT
      country_name,
      concat_ws(', ', sort_array(collect_list(Year))) as pefa2011_years
    FROM
      prd_mega.indicator_intermediate.pefa_2011_silver
    GROUP BY
      1
  ),
  subnat_hd as (
    SELECT
      country_name,
      CAST(min(year) AS INT) as subnat_edu_health_index_earliest_year,
      CAST(max(year) AS INT) as subnat_edu_health_index_latest_year,
      count(distinct adm1_name) as subnat_edu_health_index_num_subnat_regions
    FROM
      prd_mega.indicator.global_data_lab_hd_index
    WHERE
      health_index is not null
      and education_index is not null
    GROUP BY
      1
  ),
  subnat_hd_attendace as (
    SELECT
      country_name,
      CAST(min(year) AS INT) as subnat_edu_attendance_earliest_year,
      CAST(max(year) AS INT) as subnat_edu_attendance_latest_year,
      count(distinct adm1_name) as subnat_attendance_num_subnat_regions
    FROM
      prd_mega.indicator.global_data_lab_hd_index
    WHERE
      attendance is not null
    GROUP BY
      1
  ),
  youth_lit as (
    SELECT
      country_name,
      min(year) as youth_lit_rate_earliest_year,
      max(year) as youth_lit_rate_latest_year
    FROM
      prd_mega.indicator.youth_literacy_rate_unesco
    GROUP BY
      1
  ),
  edu_pov as (
    SELECT
      country_name,
      min(year) as learn_pov_earliest_year,
      max(year) as learn_pov_latest_year
    FROM
      prd_mega.indicator.learning_poverty_rate
    GROUP BY
      1
  ),
  health_cov as (
    SELECT
      country_name,
      min(year) as uni_health_coverage_earliest_year,
      max(year) as uni_health_coverage_latest_year
    FROM
      prd_mega.indicator.universal_health_coverage_index_gho
    WHERE
      universal_health_coverage_index is not null
    GROUP BY
      1
  ),
  subnat_pov as (
    SELECT
      country_name,
      min(year) as subnat_poverty_earliest_year,
      max(year) as subnat_poverty_latest_year,
      count(distinct region_name) as subnat_poverty_num_subnat_regions
    FROM
      prd_mega.indicator.subnational_poverty_index
    WHERE
      poor215 is not null
    GROUP BY
      1
  ),
  edu_priv_exp as (
    SELECT
      country_name,
      min(year) as edu_priv_spending_earliest_year,
      max(year) as edu_priv_spending_latest_year
    FROM
      prd_mega.indicator.edu_private_spending
    WHERE
      edu_private_spending_share_gdp is not null
    GROUP BY
      1
  ),
  edu_exp as (
    SELECT
      country_name,
      min(year) as edu_spending_earliest_year,
      max(year) as edu_spending_latest_year
    FROM
      prd_mega.indicator.edu_spending
    WHERE
      edu_spending_current_lcu_icp is not null
    GROUP BY
      1
  ),
  health_priv_exp as (
    SELECT
      country_name,
      min(year) as health_ooo_spending_earliest_year,
      max(year) as health_ooo_spending_latest_year
    FROM
      prd_mega.indicator.health_expenditure
    WHERE
      oop_per_capita_usd is not null
    GROUP BY
      1
  ),
  boost_subnat as (
    SELECT
      country_name,
      min(year) as boost_subnat_earliest_year,
      max(year) as boost_subnat_latest_year
    FROM
      prd_mega.boost_intermediate.quality_total_subnat_gold
    WHERE
      executed is not null
    GROUP BY
      1
  ),
  energy_generation as (
    select
      country_name,
      min(year) as energy_generation_earliest_year,
      max(year) as energy_generation_latest_year
    FROM
      prd_mega.indicator.energy_generation
    GROUP BY
      1
  ),
  energy_generation_solar_wind as (
    select
      country_name,
      min(year) as energy_generation_solar_wind_earliest_year,
      max(year) as energy_generation_solar_wind_latest_year
    FROM
      prd_mega.indicator.energy_generation
    where
      primary_fuel_type in ("Wind", "Solar")
    GROUP BY
      1
  )
  SELECT
    t.country_name,
    t.boost_earliest_year,
    t.boost_latest_year,
    CASE 
        WHEN t.country_name IN (
            'Afghanistan',
            'Albania',
            'Armenia',
            'Benin',
            'Brazil',
            'Burkina Faso',
            'Burundi',
            'Chile',
            'Croatia',
            'Colombia',
            'Dominican Republic',
            'Guatemala',
            'Haiti',
            'Kenya',
            'Kiribati',
            'Liberia',
            'Mali',
            'Mauritania',
            'Mexico',
            'Moldova',
            'Niger',
            'Paraguay',
            'Peru',
            'Poland',
            'Senegal',
            'Seychelles',
            'Solomon Islands',
            'South Africa',
            'Togo',
            'Tunisia',
            'Uganda',
            'Ukraine',
            'Uruguay'
        ) THEN 'Yes' ELSE 'No'
    END AS boost_public,
    CASE WHEN m.country_name IS NULL THEN 'No' ELSE 'Yes' END as avail_on_mega,
    f.boost_num_func_cofog,
    bsub.boost_subnat_earliest_year,
    bsub.boost_subnat_latest_year,
    p11.pefa2011_years,
    p16.pefa2016_years,
    epe.edu_priv_spending_earliest_year as edu_priv_exp_oecd_earliest_year,
    epe.edu_priv_spending_latest_year as edu_priv_exp_oecd_latest_year,
    ee.edu_spending_earliest_year as edu_exp_icp_earliest_year,
    ee.edu_spending_latest_year as edu_exp_icp_latest_year,
    yl.youth_lit_rate_earliest_year,
    yl.youth_lit_rate_latest_year,
    lp.learn_pov_earliest_year,
    lp.learn_pov_latest_year,
    hpe.health_ooo_spending_earliest_year,
    hpe.health_ooo_spending_latest_year,
    hc.uni_health_coverage_earliest_year,
    hc.uni_health_coverage_latest_year,
    shd.subnat_edu_health_index_earliest_year,
    shd.subnat_edu_health_index_latest_year,
    shd.subnat_edu_health_index_num_subnat_regions,
    areadata.subnat_edu_attendance_earliest_year,
    areadata.subnat_edu_attendance_latest_year,
    areadata.subnat_attendance_num_subnat_regions,
    sp.subnat_poverty_earliest_year,
    sp.subnat_poverty_latest_year,
    sp.subnat_poverty_num_subnat_regions,
    eg.energy_generation_earliest_year,
    eg.energy_generation_latest_year,
    egsw.energy_generation_solar_wind_earliest_year,
    egsw.energy_generation_solar_wind_latest_year
  FROM
    time_coverage t
    LEFT JOIN mega_coverage m on t.country_name = m.country_name
    LEFT JOIN func_coverage f on t.country_name = f.country_name
    LEFT JOIN pefa2016 p16 on t.country_name = p16.country_name
    LEFT JOIN pefa2011 p11 on t.country_name = p11.country_name
    LEFT JOIN subnat_hd shd on t.country_name = shd.country_name
    LEFT JOIN subnat_hd_attendace areadata on t.country_name = areadata.country_name
    LEFT JOIN youth_lit yl on t.country_name = yl.country_name
    LEFT JOIN edu_pov lp on t.country_name = lp.country_name
    LEFT JOIN health_cov hc on t.country_name = hc.country_name
    LEFT JOIN subnat_pov sp on t.country_name = sp.country_name
    LEFT JOIN edu_priv_exp epe on t.country_name = epe.country_name
    LEFT JOIN edu_exp ee on t.country_name = ee.country_name
    LEFT JOIN health_priv_exp hpe on t.country_name = hpe.country_name
    LEFT JOIN boost_subnat bsub on t.country_name = bsub.country_name
    LEFT JOIN energy_generation eg on t.country_name = eg.country_name
    LEFT JOIN energy_generation_solar_wind egsw on t.country_name = egsw.country_name
  ORDER BY
    t.country_name
)
