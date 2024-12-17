
CREATE OR REPLACE FUNCTION public.convertbytestombbyintegrationid(
	bytes bigint,
	integrationid integer)
    RETURNS numeric
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
    RETURN CASE
        -- Integration Id of 12 is for Teal carrier
        WHEN IntegrationId = 12 THEN ROUND(COALESCE(Bytes, 0) / 1000.0 / 1000.0, 3)
        ELSE ROUND(COALESCE(Bytes, 0) / 1024.0 / 1024.0, 3)
    END;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.get_current_customer_billing_period(
	customer_bill_period_end_day integer,
	customer_bill_period_end_hour integer)
    RETURNS TABLE(start_date timestamp without time zone, end_date timestamp without time zone)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY
    SELECT
        CASE
            WHEN customer_bill_period_end_day >= EXTRACT(DAY FROM NOW())
            THEN make_timestamp(
                EXTRACT(YEAR FROM NOW())::INTEGER,
                EXTRACT(MONTH FROM NOW())::INTEGER,
                customer_bill_period_end_day,
                customer_bill_period_end_hour,
                0,
                0
            )
            WHEN customer_bill_period_end_day < EXTRACT(DAY FROM NOW())
            THEN make_timestamp(
                EXTRACT(YEAR FROM NOW())::INTEGER,
                EXTRACT(MONTH FROM NOW())::INTEGER ,
                customer_bill_period_end_day,
                customer_bill_period_end_hour,
                0,
                0
            )
        END AS end_date,
        CASE
            WHEN customer_bill_period_end_day >= EXTRACT(DAY FROM NOW())
            THEN (make_timestamp(
                EXTRACT(YEAR FROM NOW())::INTEGER,
                EXTRACT(MONTH FROM NOW())::INTEGER,
                customer_bill_period_end_day,
                customer_bill_period_end_hour,
                0,
                0
            ) - INTERVAL '1 month')
            WHEN customer_bill_period_end_day < EXTRACT(DAY FROM NOW())
            THEN make_timestamp(
                EXTRACT(YEAR FROM NOW())::INTEGER,
                EXTRACT(MONTH FROM NOW())::INTEGER,
                customer_bill_period_end_day,
                customer_bill_period_end_hour,
                0,
                0
            )
        END AS start_date;
END;
$BODY$;

CREATE OR REPLACE VIEW public.vw_rev_service_products
 AS
 SELECT smi.dt_id,
        CASE
            WHEN smi.integration_id = 12 THEN smi.eid
            WHEN smi.integration_id = 13 THEN smi.iccid
            ELSE COALESCE(smi.msisdn, smi.iccid)
        END AS service_number,
        CASE
            WHEN smi.service_provider_id = 20 THEN smi.rev_vw_device_status
            ELSE smi.sim_status
        END AS device_status,
    smi.date_activated AS carrier_last_status_date,
    COALESCE(
        CASE
            WHEN ds.display_name::text = ANY (ARRAY['RestoredFromArchive'::character varying::text, 'Restored from archive'::character varying::text]) THEN true
            ELSE ds.is_active_status
        END, false) AS is_active_status,
    ds.should_have_billed_service,
    smi.iccid,
    smi.service_provider_id,
    smi.service_provider_display_name AS service_provider,
    smi.communication_plan,
    rs.activated_date,
    rs.disconnected_date,
    rsp.service_product_id,
        CASE
            WHEN rsp.package_id = 0 THEN NULL::integer
            ELSE rsp.package_id
        END AS package_id,
    rsp.service_id,
    rsp.product_id,
    rsp.description,
    rsp.rate,
    rsp.status AS rev_io_status,
        CASE
            WHEN rsp.status::text = 'ACTIVE'::text THEN true
            ELSE false
        END AS rev_is_active_status,
    rsp.cost,
    rsp.wholesale_description,
    rsp.quantity,
    rsp.integration_authentication_id,
    COALESCE(r.rev_customer_id,
        CASE
            WHEN c2.rev_customer_id IS NOT NULL THEN c2.rev_customer_id::text::character varying
            ELSE NULL::character varying
        END) AS rev_account_number,
    COALESCE(r.customer_name,
        CASE
            WHEN c2.rev_customer_id IS NOT NULL THEN c2.customer_name::text
            ELSE NULL::text
        END::character varying)::character varying(250) AS customer_name,
    rp.id AS rev_product_id,
    rp.product_type_id,
    rp.description AS product_description,
    rst.service_type_id,
    smi.tenant_id,
    smi.last_activated_date AS carrier_activated_date,
    c.rate_plan_name AS customer_rate_plan_name,
    smi.carrier_rate_plan_name,
    smi.customer_id
   FROM sim_management_inventory smi
     JOIN device_status ds ON ds.id = smi.device_status_id
     JOIN integration i ON i.id = smi.integration_id
     JOIN device_tenant dt ON dt.id = smi.dt_id
     LEFT JOIN rev_service rs ON rs.id = smi.rev_service_id
     LEFT JOIN customerrateplan c ON smi.customer_rate_plan_id = c.id
     LEFT JOIN customers c2 ON smi.customer_id = c2.id AND c2.rev_customer_id IS NOT NULL
     LEFT JOIN revcustomer r ON c2.rev_customer_id = r.id AND r.status::text <> 'CLOSED'::text AND r.is_active IS TRUE
     LEFT JOIN rev_service_product rsp ON rs.rev_service_id = rsp.service_id AND rsp.integration_authentication_id = rs.integration_authentication_id AND rsp.status::text = 'ACTIVE'::text AND rsp.is_active IS TRUE
     LEFT JOIN revcustomer r2 ON rsp.customer_id::text = r2.rev_customer_id::text AND rsp.integration_authentication_id = r2.integration_authentication_id AND r2.status::text <> 'CLOSED'::text AND r2.is_active IS TRUE
     LEFT JOIN integration_authentication ia ON r2.integration_authentication_id = ia.id
     LEFT JOIN rev_product rp ON rsp.product_id = rp.product_id AND rsp.integration_authentication_id = rp.integration_authentication_id
     LEFT JOIN rev_service_type rst ON rst.id = rs.rev_service_type_id AND rst.integration_authentication_id = rs.integration_authentication_id
     LEFT JOIN rev_product_type rpt ON rpt.product_type_id = rp.product_type_id AND rpt.integration_authentication_id = rp.integration_authentication_id AND (rpt.product_type_code IS NULL OR rpt.product_type_code::text ~~ 'RECURRING_%'::text);

CREATE OR REPLACE VIEW public.vw_revenue_assurance_group
 AS
 WITH unique_products AS (
         SELECT DISTINCT vw_rev_service_products.tenant_id,
            vw_rev_service_products.rev_account_number,
            vw_rev_service_products.customer_name,
            vw_rev_service_products.service_number,
            vw_rev_service_products.is_active_status,
            vw_rev_service_products.rev_is_active_status
           FROM vw_rev_service_products
        ), aggregated_rsp1 AS (
         SELECT unique_products.tenant_id,
            unique_products.rev_account_number,
            unique_products.customer_name,
            count(1) AS total_device_count
           FROM unique_products
          GROUP BY unique_products.tenant_id, unique_products.rev_account_number, unique_products.customer_name
        ), aggregated_rsp2 AS (
         SELECT unique_products.tenant_id,
            unique_products.rev_account_number,
            unique_products.customer_name,
            sum(
                CASE
                    WHEN unique_products.is_active_status <> unique_products.rev_is_active_status THEN 1
                    ELSE 0
                END) AS variance_count,
            sum(
                CASE
                    WHEN unique_products.is_active_status IS TRUE OR unique_products.rev_is_active_status IS TRUE THEN 1
                    ELSE 0
                END) AS any_active_count,
            sum(
                CASE
                    WHEN unique_products.rev_is_active_status IS TRUE THEN 1
                    ELSE 0
                END) AS rev_active_count
           FROM unique_products
          GROUP BY unique_products.tenant_id, unique_products.rev_account_number, unique_products.customer_name
        )
 SELECT gen_random_uuid() AS id,
    rsp1a.tenant_id,
    COALESCE(rsp1a.rev_account_number, 'Unassigned'::character varying) AS rev_customer_id,
    COALESCE(rsp1a.customer_name, 'Unassigned'::character varying) AS rev_customer_name,
    rsp2a.rev_active_count AS rev_active_device_count,
    rsp1a.total_device_count AS rev_total_device_count,
    rsp2a.any_active_count AS carrier_total_device_count,
    rsp2a.variance_count
   FROM aggregated_rsp1 rsp1a
     JOIN aggregated_rsp2 rsp2a ON rsp1a.tenant_id = rsp2a.tenant_id AND COALESCE(rsp1a.rev_account_number, ''::character varying)::text = COALESCE(rsp2a.rev_account_number, ''::character varying)::text;

CREATE OR REPLACE VIEW public.vw_rev_assurance_list_view_with_count
 AS
 SELECT DISTINCT vrsp.service_number,
    vrsp.device_status,
    vrsp.carrier_last_status_date,
    vrsp.is_active_status,
    vrsp.should_have_billed_service,
    vrsp.iccid,
    vrsp.service_provider_id,
    vrsp.service_provider,
    vrsp.communication_plan,
    vrsp.activated_date,
    vrsp.disconnected_date,
    vrsp.service_product_id,
    vrsp.package_id,
    vrsp.service_id,
    vrsp.product_id,
    vrsp.description,
    vrsp.rate,
    vrsp.rev_io_status,
    vrsp.rev_is_active_status,
    vrsp.cost,
    vrsp.wholesale_description,
    vrsp.quantity,
    vrsp.integration_authentication_id,
    vrsp.rev_account_number,
    vrsp.customer_name,
    vrsp.rev_product_id,
    vrsp.product_type_id,
    vrsp.product_description,
    vrsp.service_type_id,
    vrsp.tenant_id,
    vrsp.carrier_activated_date,
    vrsp.customer_rate_plan_name,
    vrsp.carrier_rate_plan_name AS carrier_rate_plan,
    vrag.rev_customer_name,
    vrag.rev_active_device_count,
    vrag.rev_total_device_count,
    vrag.carrier_total_device_count,
    vrag.variance_count,
    vrsp.customer_id,
    row_number() OVER (ORDER BY vrsp.customer_id) AS id
   FROM vw_rev_service_products vrsp
     JOIN vw_revenue_assurance_group vrag ON vrsp.rev_account_number::text = vrag.rev_customer_id::text AND vrsp.tenant_id = vrag.tenant_id;


CREATE OR REPLACE VIEW public.automation_rule_verification_report_view
 AS
 SELECT row_number() OVER (ORDER BY art.id)::integer AS id,
    art.automation_rule_id,
    ar.automation_rule_name AS rule_name,
    art.rule_action_id,
    ar.service_provider_name AS carrier,
    ar.description,
    art.created_date,
    art.is_active,
        CASE
            WHEN art.rule_action_value::text ~ '[A-Za-z]'::text THEN art.rule_action_value
            ELSE NULL::character varying
        END AS apply1,
        CASE
            WHEN art.rule_action_value::text ~ '^[0-9]+$'::text THEN ( SELECT ds.description
               FROM device_status ds
              WHERE ds.id = art.rule_action_value::integer)
            ELSE NULL::character varying
        END AS apply2,
        CASE
            WHEN art.rule_condition_value::text ~ '[A-Za-z]'::text THEN art.rule_condition_value
            ELSE NULL::character varying
        END AS apply_trigger_1,
        CASE
            WHEN art.rule_condition_value::text ~ '^[0-9]+$'::text THEN ( SELECT ds.description
               FROM device_status ds
              WHERE ds.id = art.rule_condition_value::integer)
            ELSE NULL::character varying
        END AS apply_trigger_2,
        CASE
            WHEN art.rule_action_id = 2 THEN
            CASE
                WHEN art.rule_action_value::text ~ '^[0-9]+$'::text THEN ( SELECT ds.description
                   FROM device_status ds
                  WHERE ds.id = art.rule_action_value::integer
                 LIMIT 1)
                ELSE art.rule_action_value
            END
            ELSE NULL::character varying
        END AS remove_trigger_1,
        CASE
            WHEN art.rule_action_id = 2 THEN
            CASE
                WHEN art.rule_condition_value::text ~ '^[0-9]+$'::text THEN ( SELECT ds.description
                   FROM device_status ds
                  WHERE ds.id = art.rule_condition_value::integer)
                ELSE art.rule_condition_value
            END
            ELSE NULL::character varying
        END AS remove_trigger_2
   FROM automation_rule_detail art
     LEFT JOIN automation_rule ar ON ar.id = art.automation_rule_id;

CREATE OR REPLACE VIEW public.bulk_chage_att_telegence_summary
 AS
 SELECT DISTINCT crp.service_provider_id,
    crp.service_provider,
    crp.rate_plan_code AS carrier_rate_plan_code,
    concat(crp2.name, '-', crp2.id) AS customer_rate_pool_name_id,
    concat(ccp.rate_plan_name, '-', ccp.rate_plan_code) AS customerrateplan_rate_plan_name_code,
    smcp.communication_plan_name,
    row_number() OVER (ORDER BY crp.service_provider_id) AS id
   FROM carrier_rate_plan crp
     LEFT JOIN customer_rate_pool crp2 ON crp.service_provider_id = crp2.service_provider_id
     LEFT JOIN customerrateplan ccp ON crp.service_provider_id = ccp.service_provider_id
     LEFT JOIN sim_management_communication_plan smcp ON crp.service_provider_id = smcp.service_provider_id
  WHERE crp.service_provider_id = 1;





CREATE OR REPLACE VIEW public.vw_automation_rule_log_list_view
 AS
 SELECT autorule.automation_rule_name,
    autorule.service_provider_id,
    serviceprovider.display_name AS service_provider_display_name,
    row_number() OVER (ORDER BY rulelog.id)::integer AS id,
    rulelog.status,
    rulelog.device_updated AS sim,
    td.subscriber_number,
    rulelog.description,
    rulelog.request_body,
    rulelog.response_body,
    rulelog.instance_id,
    rulelog.created_date,
    rulelog.created_by,
    row_number() OVER (PARTITION BY rulelog.instance_id ORDER BY rulelog.created_date) AS step_order
   FROM automation_rule autorule
     JOIN automation_rule_log rulelog ON autorule.id = rulelog.automation_rule_id
     JOIN serviceprovider serviceprovider ON serviceprovider.id = autorule.service_provider_id
     JOIN telegence_device td ON td.iccid::text = rulelog.device_updated;

CREATE OR REPLACE VIEW public.vw_combined_device_inventory_export
 AS
 SELECT combined_data.id,
    combined_data.service_provider_id,
    combined_data.service_provider_display_name,
    combined_data.integration_id,
    combined_data.iccid,
    combined_data.msisdn,
    combined_data.imei,
    combined_data.carrier_rate_plan_name,
    combined_data.carrier_rate_plan_display_rate,
    combined_data.carrier_cycle_usage_bytes,
    combined_data.carrier_cycle_usage_mb,
    combined_data.date_added,
    combined_data.date_activated,
    combined_data.created_date,
    combined_data.created_by,
    combined_data.modified_by,
    combined_data.modified_date,
    combined_data.deleted_by,
    combined_data.deleted_date,
    combined_data.is_active,
    combined_data.account_number,
    combined_data.carrier_rate_plan_id,
    combined_data.cost_center,
    combined_data.sim_status AS status_code,
    combined_data.sim_status,
    device_status.status_color,
    device_status.status_color_code,
    combined_data.tenant_id,
    serviceprovider.tenant_id AS service_provider_tenant_id,
    combined_data.rev_customer_id,
    combined_data.rev_customer_name,
    combined_data.rev_parent_customer_id,
    combined_data.foundation_account_number,
    combined_data.billing_account_number,
    combined_data.service_zip_code,
    combined_data.rate_plan_soc,
    combined_data.rate_plan_soc_description,
    combined_data.data_group_id,
    combined_data.pool_id,
    combined_data.next_bill_cycle_date,
    combined_data.device_make,
    combined_data.device_model,
    combined_data.contract_status,
    combined_data.ban_status,
    combined_data.sms_count,
    combined_data.minutes_used,
    combined_data.imei_type_id,
    combined_data.plan_limit_mb,
    combined_data.customer_id,
    combined_data.parent_customer_id,
    combined_data.customer_name,
    combined_data.customer_rate_plan_code,
    combined_data.customer_rate_plan_name,
    customerrateplan.display_rate AS customer_rate_plan_display_rate,
    combined_data.customer_data_allocation_mb,
    combined_data.username,
    combined_data.customer_rate_pool_id,
    combined_data.customer_rate_pool_name,
    combined_data.billing_cycle_start_date,
    combined_data.billing_cycle_end_date,
    combined_data.is_active_status,
    combined_data.customer_rate_plan_mb,
    combined_data.telegence_features AS telegence_feature,
    combined_data.ebonding_features,
    combined_data.ip_address,
    combined_data.customer_cycle_usage_mb,
    combined_data.eid,
    combined_data.communication_plan
   FROM sim_management_inventory combined_data
     JOIN serviceprovider ON combined_data.service_provider_id = serviceprovider.id
     LEFT JOIN customerrateplan ON combined_data.customer_rate_plan_id = customerrateplan.id
     LEFT JOIN device_status ON device_status.id = combined_data.device_status_id;

CREATE OR REPLACE VIEW public.vw_customer_pool_aggregate_usage
 AS
 WITH combined_data AS (
         SELECT customer_rate_pool.id AS customer_rate_pool_id,
            customer_rate_pool.name AS customer_rate_pool_name,
            customer_rate_pool.service_provider_id,
            serviceprovider.display_name AS service_provider_name,
            customer_rate_pool.tenant_id,
            sum(COALESCE(mobility_device.carrier_cycle_usage::numeric, 0.0)) AS data_usage_bytes,
            sum(COALESCE(mobility_device_tenant.customer_data_allocation_mb, customerrateplan.plan_mb)) AS customer_data_allocation_mb,
            count(*) AS num_records,
            serviceprovider.integration_id
           FROM customer_rate_pool customer_rate_pool
             JOIN mobility_device_tenant mobility_device_tenant ON mobility_device_tenant.customer_rate_pool_id = customer_rate_pool.id
             JOIN mobility_device mobility_device ON mobility_device_tenant.mobility_device_id = mobility_device.id AND mobility_device.service_provider_id = customer_rate_pool.service_provider_id
             JOIN serviceprovider serviceprovider ON customer_rate_pool.service_provider_id = serviceprovider.id
             JOIN customerrateplan customerrateplan ON mobility_device_tenant.customer_rate_plan_id = customerrateplan.id
          WHERE customer_rate_pool.is_active = true AND mobility_device.is_active = true  AND serviceprovider.is_active = true AND customerrateplan.is_active = true
          GROUP BY customer_rate_pool.id, customer_rate_pool.name, customer_rate_pool.service_provider_id, serviceprovider.display_name, customer_rate_pool.tenant_id, serviceprovider.integration_id
        UNION ALL
         SELECT customer_rate_pool.id AS customer_rate_pool_id,
            customer_rate_pool.name AS customer_rate_pool_name,
            customer_rate_pool.service_provider_id,
            serviceprovider.display_name AS service_provider_name,
            customer_rate_pool.tenant_id,
            sum(COALESCE(device.carrier_cycle_usage::numeric, 0.0)) AS data_usage_bytes,
            sum(COALESCE(device_tenant.customer_data_allocation_mb, customerrateplan.plan_mb)) AS customer_data_allocation_mb,
            count(*) AS num_records,
            serviceprovider.integration_id
           FROM customer_rate_pool customer_rate_pool
             JOIN device_tenant device_tenant ON device_tenant.customer_rate_pool_id = customer_rate_pool.id
             JOIN device device ON device_tenant.device_id = device.id AND device.service_provider_id = customer_rate_pool.service_provider_id
             JOIN serviceprovider serviceprovider ON customer_rate_pool.service_provider_id = serviceprovider.id
             JOIN customerrateplan customerrateplan ON device_tenant.customer_rate_plan_id = customerrateplan.id
          WHERE customer_rate_pool.is_active = true AND device.is_active = true AND serviceprovider.is_active = true  AND customerrateplan.is_active = true
          GROUP BY customer_rate_pool.id, customer_rate_pool.name, customer_rate_pool.service_provider_id, serviceprovider.display_name, customer_rate_pool.tenant_id, serviceprovider.integration_id
        )
 SELECT combined_data.customer_rate_pool_id,
    combined_data.customer_rate_pool_name,
    combined_data.service_provider_id,
    combined_data.service_provider_name,
    combined_data.tenant_id,
    combined_data.data_usage_bytes,
    combined_data.customer_data_allocation_mb,
    combined_data.num_records,
    combined_data.integration_id,
    row_number() OVER (ORDER BY (( SELECT NULL::text))) AS id
   FROM combined_data;


CREATE OR REPLACE VIEW public.vw_m2m_customer_pool_aggregate_usage
 AS
 SELECT customer_rate_pool.id AS customer_rate_pool_id,
    customer_rate_pool.name AS customer_rate_pool_name,
    customer_rate_pool.service_provider_id,
    serviceprovider.display_name AS service_provider_name,
    customer_rate_pool.tenant_id,
    sum(COALESCE(device.carrier_cycle_usage::numeric, 0.0)) AS data_usage_bytes,
    sum(COALESCE(device_tenant.customer_data_allocation_mb, customerrateplan.plan_mb)) AS customer_data_allocation_mb,
    count(*) AS num_records,
    serviceprovider.integration_id,
    row_number() OVER (ORDER BY customer_rate_pool.id) AS id
   FROM customer_rate_pool
     JOIN device_tenant ON device_tenant.customer_rate_pool_id = customer_rate_pool.id
     JOIN device ON device_tenant.device_id = device.id
     JOIN serviceprovider ON customer_rate_pool.service_provider_id = serviceprovider.id
     JOIN customerrateplan ON device_tenant.customer_rate_plan_id = customerrateplan.id
  WHERE customer_rate_pool.is_active = true AND device.is_active = true AND serviceprovider.is_active = true AND customerrateplan.is_active = true
  GROUP BY customer_rate_pool.id, customer_rate_pool.name, customer_rate_pool.service_provider_id, serviceprovider.display_name, customer_rate_pool.tenant_id, serviceprovider.integration_id;



CREATE OR REPLACE VIEW public.vw_customer_rate_pool_usage_report
 AS
 SELECT gen_random_uuid() AS id,
    customer_rate_pool.customer_rate_pool_id,
    customer_rate_pool.customer_rate_pool_name,
    customer_rate_pool.customer_rate_pool_usage_mb,
    customer_rate_pool.customer_rate_pool_allocated_mb,
    customer_rate_pool.customer_rate_pool_allocated_mb - customer_rate_pool.customer_rate_pool_usage_mb AS customer_rate_pool_data_remaining,
        CASE
            WHEN COALESCE(customer_rate_pool.customer_rate_pool_allocated_mb, 0::numeric) = 0::numeric THEN 0::numeric
            ELSE (customer_rate_pool.customer_rate_pool_usage_mb * 100::numeric / customer_rate_pool.customer_rate_pool_allocated_mb)::numeric(18,2)
        END AS customer_rate_pool_data_usage_percentage,
    customer_rate_pool.customer_rate_pool_device_count,
    customer_rate_pool.customer_rate_pool_tenant_id,
    serviceprovider.id AS service_provider_id,
    serviceprovider.display_name AS service_provider_name,
    integration.portal_type_id,
    mobility_device.msisdn AS subscriber_number,
        CASE
            WHEN integration.id = 12 THEN round(COALESCE(mobility_device.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 4)
            ELSE round(COALESCE(mobility_device.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 4)
        END AS data_usage_mb,
    COALESCE(device_tenant.customer_data_allocation_mb, COALESCE(customerrateplan.plan_mb, 0.0)) AS data_allocation_mb,
    device_tenant.account_number,
    site_info.tenant_id,
    site_info.id AS customer_id,
    site_info.parent_customer_id,
    site_info.customer_name,
    mobility_device.username,
    customerrateplan.rate_plan_name AS customer_rate_plan_name,
    customerrateplan.overage_rate_cost,
    customerrateplan.data_per_overage_charge,
    mobility_device.iccid,
    serviceprovider.integration_id,
    billing_period.billing_cycle_end_date AS billing_period_end_date,
    billing_period.billing_cycle_start_date AS billing_period_start_date,
    device_tenant.is_active
   FROM ( SELECT vw_customer_pool_aggregate_usage.customer_rate_pool_id,
            vw_customer_pool_aggregate_usage.customer_rate_pool_name,
                CASE
                    WHEN vw_customer_pool_aggregate_usage.integration_id = 12 THEN round(vw_customer_pool_aggregate_usage.data_usage_bytes / 1000.0 / 1000.0, 4)
                    ELSE round(vw_customer_pool_aggregate_usage.data_usage_bytes / 1024.0 / 1024.0, 4)
                END AS customer_rate_pool_usage_mb,
            vw_customer_pool_aggregate_usage.customer_data_allocation_mb AS customer_rate_pool_allocated_mb,
            vw_customer_pool_aggregate_usage.num_records AS customer_rate_pool_device_count,
            vw_customer_pool_aggregate_usage.tenant_id AS customer_rate_pool_tenant_id,
            vw_customer_pool_aggregate_usage.service_provider_id
           FROM vw_customer_pool_aggregate_usage) customer_rate_pool
     JOIN mobility_device_tenant device_tenant ON device_tenant.customer_rate_pool_id = customer_rate_pool.customer_rate_pool_id
     JOIN mobility_device ON mobility_device.id = device_tenant.mobility_device_id
     JOIN serviceprovider ON mobility_device.service_provider_id = serviceprovider.id AND serviceprovider.is_active = true
     JOIN integration ON integration.id = serviceprovider.integration_id
     JOIN device_status ON device_status.id = mobility_device.device_status_id
     LEFT JOIN billing_period ON billing_period.id = mobility_device.billing_period_id
     LEFT JOIN customerrateplan ON device_tenant.customer_rate_plan_id = customerrateplan.id
     LEFT JOIN customers site_info ON device_tenant.customer_id = site_info.id AND site_info.is_active = true
  WHERE mobility_device.is_active = true  AND customerrateplan.is_active = true  AND integration.portal_type_id = 2 AND mobility_device.billing_period_id = (( SELECT billing_period_1.id
           FROM billing_period billing_period_1
          WHERE billing_period_1.service_provider_id = mobility_device.service_provider_id AND billing_period_1.is_active = true
          ORDER BY billing_period_1.bill_year DESC, billing_period_1.bill_month DESC
         LIMIT 1)) AND (device_status.is_active_status = true OR mobility_device.last_activated_date < billing_period.billing_cycle_end_date AND mobility_device.last_activated_date > billing_period.billing_cycle_start_date)
UNION ALL
 SELECT gen_random_uuid() AS id,
    customer_rate_pool.customer_rate_pool_id,
    customer_rate_pool.customer_rate_pool_name,
    customer_rate_pool.customer_rate_pool_usage_mb,
    customer_rate_pool.customer_rate_pool_allocated_mb,
    customer_rate_pool.customer_rate_pool_allocated_mb - customer_rate_pool.customer_rate_pool_usage_mb AS customer_rate_pool_data_remaining,
        CASE
            WHEN COALESCE(customer_rate_pool.customer_rate_pool_allocated_mb, 0::numeric) = 0::numeric THEN 0::numeric
            ELSE round(customer_rate_pool.customer_rate_pool_usage_mb * 100.0 / customer_rate_pool.customer_rate_pool_allocated_mb, 2)
        END AS customer_rate_pool_data_usage_percentage,
    customer_rate_pool.customer_rate_pool_device_count,
    customer_rate_pool.customer_rate_pool_tenant_id,
    serviceprovider.id AS service_provider_id,
    serviceprovider.display_name AS service_provider_name,
    integration.portal_type_id,
    device.msisdn AS subscriber_number,
        CASE
            WHEN integration.id = 12 THEN round(COALESCE(device.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 4)::numeric(25,4)
            ELSE round(COALESCE(device.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 4)::numeric(25,4)
        END AS data_usage_mb,
    COALESCE(device_tenant.customer_data_allocation_mb, COALESCE(customerrateplan.plan_mb, 0.0)) AS data_allocation_mb,
    device_tenant.account_number,
    customers.tenant_id,
    customers.id AS customer_id,
    customers.parent_customer_id,
    customers.customer_name,
    device.username,
    customerrateplan.rate_plan_name AS customer_rate_plan_name,
    customerrateplan.overage_rate_cost,
    customerrateplan.data_per_overage_charge,
    device.iccid,
    integration.id AS integration_id,
    billing_period.billing_cycle_end_date AS billing_period_end_date,
    billing_period.billing_cycle_start_date AS billing_period_start_date,
    device_tenant.is_active
   FROM ( SELECT vw_m2m_customer_pool_aggregate_usage.customer_rate_pool_id,
            vw_m2m_customer_pool_aggregate_usage.customer_rate_pool_name,
                CASE
                    WHEN vw_m2m_customer_pool_aggregate_usage.integration_id = 12 THEN round(vw_m2m_customer_pool_aggregate_usage.data_usage_bytes / 1000.0 / 1000.0, 4)::numeric(25,4)
                    ELSE round(vw_m2m_customer_pool_aggregate_usage.data_usage_bytes / 1024.0 / 1024.0, 4)::numeric(25,4)
                END AS customer_rate_pool_usage_mb,
            vw_m2m_customer_pool_aggregate_usage.customer_data_allocation_mb AS customer_rate_pool_allocated_mb,
            vw_m2m_customer_pool_aggregate_usage.num_records AS customer_rate_pool_device_count,
            vw_m2m_customer_pool_aggregate_usage.tenant_id AS customer_rate_pool_tenant_id,
            vw_m2m_customer_pool_aggregate_usage.service_provider_id
           FROM vw_m2m_customer_pool_aggregate_usage) customer_rate_pool
     JOIN device_tenant ON device_tenant.customer_rate_pool_id = customer_rate_pool.customer_rate_pool_id
     JOIN device ON device.id = device_tenant.device_id
     JOIN serviceprovider ON customer_rate_pool.service_provider_id = serviceprovider.id AND serviceprovider.is_active = true
     JOIN integration ON integration.id = serviceprovider.integration_id
     JOIN billing_period ON billing_period.id = device.billing_period_id
     LEFT JOIN customerrateplan ON device_tenant.customer_rate_plan_id = customerrateplan.id
     LEFT JOIN customers ON device_tenant.customer_id = customers.id AND customers.is_active = true
     LEFT JOIN revcustomer ON customers.rev_customer_id = revcustomer.id AND revcustomer.is_active = true
  WHERE device.is_active = true AND customerrateplan.is_active = true;



CREATE OR REPLACE VIEW public.vw_daily_usage_report
 AS
 SELECT row_number() OVER (ORDER BY sm.id)::integer AS id,
    sm.service_provider_display_name AS service_provider,
    bp.billing_cycle_end_date,
    sm.iccid,
    sm.msisdn,
    sm.foundation_account_number,
    sm.billing_account_number,
    sm.customer_rate_pool_name AS customer_pool,
    sm.customer_name,
    sm.username,
    sm.carrier_rate_plan_name AS carrier_rate_plan,
    sm.customer_rate_plan_name AS customer_rate_plan,
    sp.integration_id,
        CASE
            WHEN sp.integration_id = 12 THEN round(COALESCE(sm.carrier_cycle_usage_bytes::numeric, 0.0) / 1000.0 / 1000.0, 3)
            ELSE round(COALESCE(sm.carrier_cycle_usage_bytes::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END AS datausagemb,
    sm.carrier_cycle_usage_bytes AS carrier_cycle_usage,
    sm.sim_status,
    sm.date_activated,
    sm.created_date,
    sm.modified_date,
    sm.is_active
   FROM sim_management_inventory sm
     JOIN serviceprovider sp ON sm.service_provider_id = sp.id
     LEFT JOIN billing_period bp ON sm.billing_period_id = bp.id;

CREATE OR REPLACE VIEW public.vw_device_bulk_change
 AS
 SELECT row_number() OVER (ORDER BY bc.id) AS id,
    rt.id AS change_request_type_id,
    rt.code AS change_request_type_code,
    rt.display_name AS change_request_type_display_name,
    bc.status,
    bc.tenant_id,
    bc.service_provider_id,
    sp.display_name AS service_provider_display_name,
    sp.integration_id,
    i.portal_type_id,
    s.id AS customer_id,
    s.parent_customer_id,
    s.customer_name,
    af.id AS file_id,
    af.amazon_file_name,
    af.file_name,
    bc.processed_date,
    bc.processed_by,
    ( SELECT count(1) AS count
           FROM ( SELECT 1 AS change
                   FROM m2m_device_change m2m
                  WHERE m2m.bulk_change_id = bc.id
                UNION ALL
                 SELECT 1 AS change
                   FROM mobility_device_change mbl
                  WHERE mbl.bulk_change_id = bc.id
                UNION ALL
                 SELECT 1 AS change
                   FROM lnp_device_change lnpdc
                  WHERE lnpdc.bulk_change_id = bc.id) changes) AS change_count,
    ( SELECT count(1) AS count
           FROM ( SELECT 1 AS change
                   FROM m2m_device_change m2m
                  WHERE m2m.bulk_change_id = bc.id AND m2m.is_processed = false
                UNION ALL
                 SELECT 1 AS change
                   FROM mobility_device_change mbl
                  WHERE mbl.bulk_change_id = bc.id AND mbl.is_processed = false
                UNION ALL
                 SELECT 1 AS change
                   FROM lnp_device_change lnpdc
                  WHERE lnpdc.bulk_change_id = bc.id AND lnpdc.is_processed = false) changes) AS remaining_count,
    ( SELECT count(1) AS count
           FROM ( SELECT 1 AS change
                   FROM m2m_device_change m2m
                  WHERE m2m.bulk_change_id = bc.id AND m2m.is_processed = true AND m2m.has_errors = false
                UNION ALL
                 SELECT 1 AS change
                   FROM mobility_device_change mbl
                  WHERE mbl.bulk_change_id = bc.id AND mbl.is_processed = true AND mbl.has_errors = false
                UNION ALL
                 SELECT 1 AS change
                   FROM lnp_device_change lnpdc
                  WHERE lnpdc.bulk_change_id = bc.id AND lnpdc.is_processed = true AND lnpdc.has_errors = false) changes) AS processed_count,
    ( SELECT count(1) AS count
           FROM ( SELECT 1 AS change
                   FROM m2m_device_change m2m
                  WHERE m2m.bulk_change_id = bc.id AND m2m.is_processed = true AND m2m.has_errors = true
                UNION ALL
                 SELECT 1 AS change
                   FROM mobility_device_change mbl
                  WHERE mbl.bulk_change_id = bc.id AND mbl.is_processed = true AND mbl.has_errors = true
                UNION ALL
                 SELECT 1 AS change
                   FROM lnp_device_change lnpdc
                  WHERE lnpdc.bulk_change_id = bc.id AND lnpdc.is_processed = true AND lnpdc.has_errors = true) changes) AS error_count,
    bc.created_date,
    bc.created_by,
    bc.modified_by,
    bc.modified_date,
    bc.deleted_by,
    bc.deleted_date,
    bc.is_active
   FROM sim_management_bulk_change bc
     JOIN sim_management_bulk_change_type rt ON bc.change_request_type_id = rt.id AND rt.is_active = true
     JOIN serviceprovider sp ON bc.service_provider_id = sp.id AND sp.is_active = true
     JOIN integration i ON i.id = sp.integration_id AND i.is_active = true
     LEFT JOIN customers s ON bc.customer_id = s.id AND s.is_active = true
     LEFT JOIN app_file af ON bc.app_file_id = af.id AND af.is_active = true
  WHERE bc.is_active = true ;


CREATE OR REPLACE VIEW public.vw_device_high_usage_scatter_chart
 AS
 SELECT row_number() OVER (ORDER BY dt.id)::integer AS id,
    dt.device_id,
        CASE
            WHEN i.id = 12 THEN round(smi.carrier_cycle_usage::numeric / 1000.0 / 1000.0, 3)
            ELSE round(smi.carrier_cycle_usage::numeric / 1024.0 / 1024.0, 3)
        END AS ctd_data_usage_mb,
    smi.ctd_session_count,
    dt.account_number,
    smi.iccid,
    smi.msisdn,
    smi.service_provider_id,
    sp.service_provider_name,
    c.tenant_id AS customer_tenant_id,
    i.portal_type_id,
    c.id AS customer_id,
    c.parent_customer_id,
    c.customer_name
   FROM device smi
     JOIN device_tenant dt ON dt.device_id = smi.id
     JOIN serviceprovider sp ON sp.id = smi.service_provider_id
     LEFT JOIN customers c ON dt.customer_id = c.id
     LEFT JOIN integration i ON i.id = sp.integration_id
  WHERE (smi.sim_status::text = 'ACTIVATED'::text OR smi.sim_status::text = 'active'::text OR smi.sim_status::text = 'A'::text) AND smi.carrier_cycle_usage > 0 AND smi.is_active = true;


CREATE OR REPLACE VIEW public.vw_device_status_trend_by_month
 AS
 SELECT COALESCE(gen_random_uuid(), '00000000-0000-0000-0000-000000000000'::uuid) AS id,
    jdsa.activated_count,
    jdsa.activation_ready_count,
    jdsa.deactivated_count,
    jdsa.inventory_count,
    jdsa.retired_count,
    jdsa.test_ready_count,
    to_char(jdsa.created_date, 'YYYY-MM-DD'::text) AS created_date,
    jdsa.bill_year,
    jdsa.bill_month,
    jdsa.service_provider_id,
    sp.service_provider_name AS service_provider,
    "int".portal_type_id
   FROM jasper_device_sync_audit jdsa
     JOIN serviceprovider sp ON jdsa.service_provider_id = sp.id
     JOIN ( SELECT jasper_device_sync_audit.bill_year,
            jasper_device_sync_audit.bill_month,
            jasper_device_sync_audit.service_provider_id,
            max(jasper_device_sync_audit.id) AS id
           FROM jasper_device_sync_audit
          GROUP BY jasper_device_sync_audit.bill_year, jasper_device_sync_audit.bill_month, jasper_device_sync_audit.service_provider_id) jdsa_month ON jdsa.id = jdsa_month.id AND jdsa_month.service_provider_id = sp.id
     LEFT JOIN integration "int" ON "int".id = sp.integration_id
UNION
 SELECT COALESCE(gen_random_uuid(), '00000000-0000-0000-0000-000000000000'::uuid) AS id,
    COALESCE(jdsa.active_count, 0) + COALESCE(jdsa.pending_mdn_change_count, 0) + COALESCE(jdsa.pending_prl_update_count, 0) + COALESCE(jdsa.pending_service_plan_change_count, 0) + COALESCE(jdsa.pending_account_update_count, 0) AS activated_count,
    COALESCE(jdsa.pending_resume_count, 0) + COALESCE(jdsa.pending_preactive_count, 0) + COALESCE(jdsa.pending_activation_count, 0) AS activation_ready_count,
    COALESCE(jdsa.deactive_count, 0) + COALESCE(jdsa.suspend_count, 0) + COALESCE(jdsa.pending_deactivation_count, 0) + COALESCE(jdsa.pending_suspend_count, 0) AS deactivated_count,
    0 AS inventory_count,
    0 AS retired_count,
    COALESCE(jdsa.pre_active_count, 0) AS test_ready_count,
    to_char(jdsa.created_date, 'YYYY-MM-DD'::text) AS created_date,
    jdsa.bill_year,
    jdsa.bill_month,
    jdsa.service_provider_id,
    sp.service_provider_name AS service_provider,
    "int".portal_type_id
   FROM thing_space_device_sync_audit jdsa
     JOIN serviceprovider sp ON sp.id = jdsa.service_provider_id
     JOIN ( SELECT thing_space_device_sync_audit.bill_year,
            thing_space_device_sync_audit.bill_month,
            max(thing_space_device_sync_audit.id) AS id
           FROM thing_space_device_sync_audit
          GROUP BY thing_space_device_sync_audit.bill_year, thing_space_device_sync_audit.bill_month) jdsa_month ON jdsa.id = jdsa_month.id
     LEFT JOIN integration "int" ON "int".id = sp.integration_id
UNION
 SELECT COALESCE(gen_random_uuid(), '00000000-0000-0000-0000-000000000000'::uuid) AS id,
    COALESCE(jdsa.active_count, 0) AS activated_count,
    0 AS activation_ready_count,
    COALESCE(jdsa.suspend_count, 0) AS deactivated_count,
    0 AS inventory_count,
    0 AS retired_count,
    0 AS test_ready_count,
    to_char(jdsa.created_date, 'YYYY-MM-DD'::text) AS created_date,
    jdsa.bill_year,
    jdsa.bill_month,
    jdsa.service_provider_id,
    sp.service_provider_name AS service_provider,
    "int".portal_type_id
   FROM telegence_device_sync_audit jdsa
     JOIN serviceprovider sp ON sp.id = jdsa.service_provider_id
     JOIN ( SELECT telegence_device_sync_audit.bill_year,
            telegence_device_sync_audit.bill_month,
            max(telegence_device_sync_audit.id) AS id
           FROM telegence_device_sync_audit
          GROUP BY telegence_device_sync_audit.bill_year, telegence_device_sync_audit.bill_month) jdsa_month ON jdsa.id = jdsa_month.id
     LEFT JOIN integration "int" ON "int".id = sp.integration_id
UNION
 SELECT COALESCE(gen_random_uuid(), '00000000-0000-0000-0000-000000000000'::uuid) AS id,
    COALESCE(jdsa.active_count, 0) AS activated_count,
    0 AS activation_ready_count,
    COALESCE(jdsa.suspend_count, 0) AS deactivated_count,
    0 AS inventory_count,
    0 AS retired_count,
    0 AS test_ready_count,
    to_char(jdsa.created_date, 'YYYY-MM-DD'::text) AS created_date,
    jdsa.bill_year,
    jdsa.bill_month,
    jdsa.service_provider_id,
    sp.service_provider_name AS service_provider,
    "int".portal_type_id
   FROM e_bonding_device_sync_audit jdsa
     JOIN serviceprovider sp ON sp.id = jdsa.service_provider_id
     JOIN ( SELECT e_bonding_device_sync_audit.bill_year,
            e_bonding_device_sync_audit.bill_month,
            max(e_bonding_device_sync_audit.id) AS id
           FROM e_bonding_device_sync_audit
          GROUP BY e_bonding_device_sync_audit.bill_year, e_bonding_device_sync_audit.bill_month) jdsa_month ON jdsa.id = jdsa_month.id
     LEFT JOIN integration "int" ON "int".id = sp.integration_id;


CREATE OR REPLACE VIEW public.vw_device_usage_trend_by_month
 AS
 SELECT du_month.service_provider_id,
    du_month.service_provider_name AS service_provider,
    COALESCE(row_number() OVER (ORDER BY du_month.bill_year, du_month.bill_month, du_month.account_number), 0::bigint) AS id,
    du_month.bill_year,
    du_month.bill_month,
    du_month.account_number,
    sum(du_month.carrier_cycle_usage) AS total_usage_bytes,
    sum(du_month.carrier_cycle_usage_mb) AS total_usage_mb,
    count(1) AS total_cards,
    avg(du_month.carrier_cycle_usage) AS avg_usage_per_card_bytes,
    avg(du_month.carrier_cycle_usage_mb) AS avg_usage_per_card_mb,
    du_month.tenant_id,
    du_month.portal_type_id,
    du_month.customer_id,
    du_month.parent_customer_id,
    du_month.customer_name
   FROM ( SELECT jd.service_provider_id,
            sp.service_provider_name,
            jduh.bill_year,
            jduh.bill_month,
            COALESCE(jduh.carrier_cycle_usage::numeric, 0.0) AS carrier_cycle_usage,
                CASE
                    WHEN i.id = 12 THEN COALESCE(jduh.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0
                    ELSE COALESCE(jduh.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0
                END AS carrier_cycle_usage_mb,
            dt.account_number,
            c.tenant_id,
            i.portal_type_id,
            c.id AS customer_id,
            c.parent_customer_id,
            c.customer_name
           FROM device jd
             JOIN device_tenant dt ON jd.id = dt.device_id
             JOIN serviceprovider sp ON sp.id = jd.service_provider_id
             JOIN device_status st ON jd.device_status_id = st.id
             LEFT JOIN customers c ON dt.customer_id = c.id
             LEFT JOIN ( SELECT dh.id,
                    bp.bill_year,
                    bp.bill_month,
                    dh.iccid,
                    dh.carrier_cycle_usage,
                    dh.msisdn,
                    dh.status,
                    dh.created_date,
                    COALESCE(sp_1.bill_period_end_day::character varying, '18'::character varying) AS billing_period_end_day,
                    COALESCE(sp_1.bill_period_end_hour::character varying, '18'::character varying) AS billing_period_end_hour,
                    dh.customer_id
                   FROM device_history dh
                     JOIN device_status st_1 ON st_1.id = dh.device_status_id
                     JOIN serviceprovider sp_1 ON sp_1.id = dh.service_provider_id
                     JOIN billing_period bp ON bp.id = dh.billing_period_id
                  WHERE st_1.is_active_status = true OR dh.carrier_cycle_usage > 0 AND dh.status::text <> 'TEST READY'::text) jduh ON jd.id = jduh.id AND dt.customer_id = jduh.customer_id
             LEFT JOIN integration i ON i.id = sp.integration_id
          WHERE jd.is_active = true  AND (st.is_active_status = true OR jd.last_activated_date IS NOT NULL AND jd.last_activated_date > (to_timestamp(((((((jduh.bill_year || '-'::text) || lpad(jduh.bill_month::text, 2, '0'::text)) || '-'::text) || lpad(jduh.billing_period_end_day::text, 2, '0'::text)) || ' '::text) || lpad(jduh.billing_period_end_hour::text, 2, '0'::text)) || ':00'::text, 'YYYY-MM-DD HH24:MI'::text) - '1 mon'::interval))) du_month
  GROUP BY du_month.service_provider_id, du_month.service_provider_name, du_month.bill_year, du_month.bill_month, du_month.account_number, du_month.tenant_id, du_month.portal_type_id, du_month.customer_id, du_month.parent_customer_id, du_month.customer_name;


CREATE OR REPLACE VIEW public.vw_m2m_device_current_billing_period
 AS
 SELECT device_tenant.device_id,
    current_customer_billing_period.end_date,
    current_customer_billing_period.start_date,
    device_tenant.tenant_id,
    row_number() OVER (ORDER BY device_tenant.device_id) AS id
   FROM device_tenant
     JOIN customers ON device_tenant.customer_id = customers.id
     LEFT JOIN LATERAL get_current_customer_billing_period(customers.customer_bill_period_end_day, customers.customer_bill_period_end_hour) current_customer_billing_period(start_date, end_date) ON true
  WHERE customers.customer_bill_period_end_day IS NOT NULL OR customers.customer_bill_period_end_hour IS NOT NULL;



CREATE OR REPLACE VIEW public.vw_m2m_customer_current_cycle_device_usage
 AS
 SELECT device_usage.m2m_device_id,
    sum(device_usage.data_usage) AS customer_cycle_usage_byte,
    min(device_usage.start_date) AS start_date,
    device_usage.end_date,
    device_usage.tenant_id,
    row_number() OVER (ORDER BY device_usage.m2m_device_id) AS id
   FROM ( SELECT device_usage_1.m2m_device_id,
            device_usage_1.data_usage,
            vw_m2m_device_current_billing_period.end_date,
            vw_m2m_device_current_billing_period.start_date,
            device_usage_1.usage_date,
            vw_m2m_device_current_billing_period.tenant_id
           FROM device_usage device_usage_1
             JOIN vw_m2m_device_current_billing_period ON vw_m2m_device_current_billing_period.device_id = device_usage_1.m2m_device_id
          WHERE device_usage_1.m2m_device_id IS NOT NULL AND device_usage_1.usage_date >= vw_m2m_device_current_billing_period.start_date AND device_usage_1.usage_date < vw_m2m_device_current_billing_period.end_date) device_usage
  GROUP BY device_usage.m2m_device_id, device_usage.end_date, device_usage.tenant_id;


CREATE OR REPLACE VIEW public.vw_mobility_device_current_billing_period
 AS
 SELECT mobility_device_tenant.mobility_device_id,
    current_customer_billing_period.end_date,
    current_customer_billing_period.start_date,
    mobility_device_tenant.tenant_id,
    row_number() OVER (ORDER BY mobility_device_tenant.mobility_device_id) AS id
   FROM mobility_device_tenant
     JOIN customers ON mobility_device_tenant.customer_id = customers.id
     CROSS JOIN LATERAL get_current_customer_billing_period(customers.customer_bill_period_end_day, customers.customer_bill_period_end_hour) current_customer_billing_period(start_date, end_date)
  WHERE customers.customer_bill_period_end_day IS NOT NULL OR customers.customer_bill_period_end_hour IS NOT NULL;


CREATE OR REPLACE VIEW public.vw_mobility_customer_current_cycle_device_usage
 AS
 SELECT device_usage.mobility_device_id,
    sum(device_usage.data_usage) AS customer_cycle_usage_byte,
    min(device_usage.start_date) AS start_date,
    device_usage.end_date,
    device_usage.tenant_id,
    row_number() OVER (ORDER BY device_usage.mobility_device_id) AS id
   FROM ( SELECT device_usage_1.mobility_device_id,
            device_usage_1.data_usage,
            vw_mobility_device_current_billing_period.end_date,
            vw_mobility_device_current_billing_period.start_date,
            device_usage_1.usage_date,
            vw_mobility_device_current_billing_period.tenant_id
           FROM device_usage device_usage_1
             JOIN vw_mobility_device_current_billing_period ON vw_mobility_device_current_billing_period.mobility_device_id = device_usage_1.mobility_device_id
          WHERE device_usage_1.mobility_device_id IS NOT NULL AND device_usage_1.usage_date >= vw_mobility_device_current_billing_period.start_date AND device_usage_1.usage_date < vw_mobility_device_current_billing_period.end_date) device_usage
  GROUP BY device_usage.mobility_device_id, device_usage.end_date, device_usage.tenant_id;


CREATE OR REPLACE VIEW public.vw_usage_by_line_report
 AS
 SELECT smih.id,
    smi.billing_cycle_end_date,
    smih.service_provider_id,
    smi.service_provider_display_name,
    smih.iccid,
    smih.msisdn,
    smih.imei,
    smih.rate_plan AS carrier_rate_plan,
    smih.carrier_cycle_usage,
        CASE
            WHEN smi.id = 12 THEN round(COALESCE(smih.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 3)
            ELSE round(COALESCE(smih.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END AS data_usage_mb,
    smih.provider_date_added AS date_added,
    smih.provider_date_activated AS date_activated,
    smi.customer_rate_plan_code,
    smi.customer_rate_plan_name,
    smi.customer_rate_plan_mb,
    smih.created_date,
    smih.created_by,
    smih.modified_by,
    smih.modified_date,
    smih.deleted_by,
    smih.deleted_date,
    smih.is_active,
    smi.account_number,
    smih.carrier_rate_plan_id,
    smih.status AS status_code,
    smi.sim_status AS status_display_name,
    smi.tenant_id,
    smi.rev_customer_id,
    smi.customer_name,
    smi.rev_parent_customer_id,
    COALESCE(smih.ctd_sms_usage, 0::bigint) AS smsusage,
    smih.device_history_id,
    smih.is_pushed,
    smi.customer_rate_pool_name,
    smi.customer_rate_plan_allows_sim_pooling,
    smi.customer_data_allocation_mb,
    smi.foundation_account_number,
    smi.service_zip_code,
    smi.rate_plan_soc,
    smi.data_group_id,
    smi.pool_id,
    smi.device_make,
    smi.device_model,
    smi.contract_status,
    smi.ban_status,
    smi.plan_limit_mb,
    smi.username,
    smi.ctd_voice_usage AS minutes_used,
    smi.ip_address,
    smi.billing_account_number,
    smi.integration_id
   FROM sim_management_inventory_history smih
     JOIN sim_management_inventory smi ON smih.smi_id = smi.id
  WHERE smih.is_active = true AND (smi.is_active_status = true OR smih.last_activated_date < smi.billing_cycle_end_date AND smih.last_activated_date > smi.billing_cycle_start_date OR smih.carrier_cycle_usage > 0);

CREATE OR REPLACE VIEW public.vw_zero_usage_report
 AS
 SELECT sim_management_inventory.id,
    sim_management_inventory.iccid,
    sim_management_inventory.msisdn,
    sim_management_inventory.carrier_rate_plan_name AS carrier_rate_plan,
    sim_management_inventory.sim_status,
    sim_management_inventory.communication_plan,
    sim_management_inventory.account_number,
    sim_management_inventory.customer_name,
    sim_management_inventory.username,
    sim_management_inventory.service_provider_display_name AS service_provider,
    sim_management_inventory.last_usage_date,
    sim_management_inventory.created_date,
    sim_management_inventory.billing_cycle_end_date,
    sim_management_inventory.is_active,
    sim_management_inventory.customer_rate_plan_name
   FROM sim_management_inventory
  WHERE sim_management_inventory.carrier_cycle_usage_bytes = 0;


CREATE OR REPLACE VIEW public.vw_status_history_report
 AS
 SELECT dsh.id,
    dsh.iccid,
    dsh.msisdn,
    dsh.current_status,
    dsh.change_event_type,
    dsh.changed_by,
        CASE
            WHEN dsh.previous_status::text = dsh.current_status::text THEN ( SELECT previous_record.current_status
               FROM device_status_history previous_record
              WHERE previous_record.device_id = dsh.device_id AND (dsh.tenant_id IS NULL OR dsh.tenant_id = previous_record.tenant_id) AND previous_record.date_of_change < dsh.date_of_change
              ORDER BY previous_record.date_of_change DESC
             LIMIT 1)
            ELSE dsh.previous_status
        END AS previous_status,
    dsh.service_provider_id,
    dsh.sim_management_inventory_id,
    dsh.bulk_change_id,
    dsh.customer_name,
    dsh.customer_account_number,
    dsh.username,
    dsh.customer_rate_plan,
    dsh.customer_rate_pool,
    COALESCE(dsh.tenant_id, dt.tenant_id) AS tenant_id,
    sp.display_name AS serviceprovidername,
    dt.customer_id,
    dt.account_number,
    d.created_date,
    dt.is_active,
    bp.billing_cycle_start_date,
    bp.billing_cycle_end_date
   FROM device_status_history dsh
     JOIN device d ON dsh.device_id = d.id
     JOIN serviceprovider sp ON sp.id = d.service_provider_id
     JOIN billing_period bp ON d.billing_period_id = bp.id
     LEFT JOIN device_tenant dt ON dt.device_id = d.id AND (dt.tenant_id = dsh.tenant_id OR dsh.tenant_id IS NULL)
UNION ALL
 SELECT dsh.id,
    dsh.iccid,
    dsh.msisdn,
    dsh.current_status,
    dsh.change_event_type,
    dsh.changed_by,
        CASE
            WHEN dsh.previous_status::text = dsh.current_status::text THEN ( SELECT previous_record.current_status
               FROM device_status_history previous_record
              WHERE previous_record.device_id = dsh.device_id AND (dsh.tenant_id IS NULL OR dsh.tenant_id = previous_record.tenant_id) AND previous_record.date_of_change < dsh.date_of_change
              ORDER BY previous_record.date_of_change DESC
             LIMIT 1)
            ELSE dsh.previous_status
        END AS previous_status,
    dsh.service_provider_id,
    dsh.sim_management_inventory_id,
    dsh.bulk_change_id,
    dsh.customer_name,
    dsh.customer_account_number,
    dsh.username,
    dsh.customer_rate_plan,
    dsh.customer_rate_pool,
    COALESCE(dsh.tenant_id, dt.tenant_id) AS tenant_id,
    sp.display_name AS serviceprovidername,
    dt.customer_id,
    dt.account_number,
    d.created_date,
    dt.is_active,
    bp.billing_cycle_start_date,
    bp.billing_cycle_end_date
   FROM device_status_history dsh
     JOIN mobility_device d ON dsh.device_id = d.id
     JOIN serviceprovider sp ON sp.id = d.service_provider_id
     JOIN billing_period bp ON d.billing_period_id = bp.id
     LEFT JOIN mobility_device_tenant dt ON dt.mobility_device_id = d.id AND (dt.tenant_id = dsh.tenant_id OR dsh.tenant_id IS NULL);

CREATE OR REPLACE VIEW public.vw_smi_sim_cards_by_customer_rate_plan_limit_report
 AS
 SELECT COALESCE(gen_random_uuid(), gen_random_uuid()) AS unique_id,
    smi.service_provider_id,
    sp.service_provider_name,
    dt.account_number,
    count(1) AS sim_count,
    sum(COALESCE(smi.carrier_cycle_usage, 0::bigint)) AS carrier_cycle_usage,
    sum(COALESCE(smi.ctd_session_count, 0::bigint)) AS ctd_session_count,
    c.tenant_id,
    i.portal_type_id,
    crp.plan_mb,
    c.id,
    c.parent_customer_id,
    c.customer_name
   FROM device smi
     JOIN device_tenant dt ON smi.id = dt.device_id
     JOIN device_status ds ON smi.device_status_id = ds.id
     JOIN serviceprovider sp ON smi.service_provider_id = sp.id
     LEFT JOIN customerrateplan crp ON crp.id = dt.customer_rate_plan_id
     LEFT JOIN customers c ON dt.customer_id = c.id
     LEFT JOIN integration i ON i.id = sp.integration_id
  WHERE smi.is_active = true  AND ds.is_active_status = true
  GROUP BY smi.service_provider_id, sp.service_provider_name, dt.account_number, c.tenant_id, i.portal_type_id, crp.plan_mb, c.id, c.parent_customer_id, c.customer_name;

CREATE OR REPLACE VIEW public.vw_rev_service_products
 AS
 SELECT smi.dt_id,
        CASE
            WHEN smi.integration_id = 12 THEN smi.eid
            WHEN smi.integration_id = 13 THEN smi.iccid
            ELSE COALESCE(smi.msisdn, smi.iccid)
        END AS service_number,
        CASE
            WHEN smi.service_provider_id = 20 THEN smi.rev_vw_device_status
            ELSE smi.sim_status
        END AS device_status,
    smi.date_activated AS carrier_last_status_date,
    COALESCE(
        CASE
            WHEN ds.display_name::text = ANY (ARRAY['RestoredFromArchive'::character varying::text, 'Restored from archive'::character varying::text]) THEN true
            ELSE ds.is_active_status
        END, false) AS is_active_status,
    ds.should_have_billed_service,
    smi.iccid,
    smi.service_provider_id,
    smi.service_provider_display_name AS service_provider,
    smi.communication_plan,
    rs.activated_date,
    rs.disconnected_date,
    rsp.service_product_id,
        CASE
            WHEN rsp.package_id = 0 THEN NULL::integer
            ELSE rsp.package_id
        END AS package_id,
    rsp.service_id,
    rsp.product_id,
    rsp.description,
    rsp.rate,
    rsp.status AS rev_io_status,
        CASE
            WHEN rsp.status::text = 'ACTIVE'::text THEN true
            ELSE false
        END AS rev_is_active_status,
    rsp.cost,
    rsp.wholesale_description,
    rsp.quantity,
    rsp.integration_authentication_id,
    COALESCE(r.rev_customer_id,
        CASE
            WHEN c2.rev_customer_id IS NOT NULL THEN c2.rev_customer_id::text::character varying
            ELSE NULL::character varying
        END) AS rev_account_number,
    COALESCE(r.customer_name,
        CASE
            WHEN c2.rev_customer_id IS NOT NULL THEN c2.customer_name::text
            ELSE NULL::text
        END::character varying)::character varying(250) AS customer_name,
    rp.id AS rev_product_id,
    rp.product_type_id,
    rp.description AS product_description,
    rst.service_type_id,
    smi.tenant_id,
    smi.last_activated_date AS carrier_activated_date,
    c.rate_plan_name AS customer_rate_plan_name,
    smi.carrier_rate_plan_name,
    smi.customer_id
   FROM sim_management_inventory smi
     JOIN device_status ds ON ds.id = smi.device_status_id
     JOIN integration i ON i.id = smi.integration_id
     JOIN device_tenant dt ON dt.id = smi.dt_id
     LEFT JOIN rev_service rs ON rs.id = smi.rev_service_id
     LEFT JOIN customerrateplan c ON smi.customer_rate_plan_id = c.id
     LEFT JOIN customers c2 ON smi.customer_id = c2.id AND c2.rev_customer_id IS NOT NULL
     LEFT JOIN revcustomer r ON c2.rev_customer_id = r.id AND r.status::text <> 'CLOSED'::text AND r.is_active IS TRUE
     LEFT JOIN rev_service_product rsp ON rs.rev_service_id = rsp.service_id AND rsp.integration_authentication_id = rs.integration_authentication_id AND rsp.status::text = 'ACTIVE'::text AND rsp.is_active IS TRUE
     LEFT JOIN revcustomer r2 ON rsp.customer_id::text = r2.rev_customer_id::text AND rsp.integration_authentication_id = r2.integration_authentication_id AND r2.status::text <> 'CLOSED'::text AND r2.is_active IS TRUE
     LEFT JOIN integration_authentication ia ON r2.integration_authentication_id = ia.id
     LEFT JOIN rev_product rp ON rsp.product_id = rp.product_id AND rsp.integration_authentication_id = rp.integration_authentication_id
     LEFT JOIN rev_service_type rst ON rst.id = rs.rev_service_type_id AND rst.integration_authentication_id = rs.integration_authentication_id
     LEFT JOIN rev_product_type rpt ON rpt.product_type_id = rp.product_type_id AND rpt.integration_authentication_id = rp.integration_authentication_id AND (rpt.product_type_code IS NULL OR rpt.product_type_code::text ~~ 'RECURRING_%'::text);


CREATE OR REPLACE VIEW public.vw_revenue_assurance_group
 AS
 WITH unique_products AS (
         SELECT DISTINCT vw_rev_service_products.tenant_id,
            vw_rev_service_products.rev_account_number,
            vw_rev_service_products.customer_name,
            vw_rev_service_products.service_number,
            vw_rev_service_products.is_active_status,
            vw_rev_service_products.rev_is_active_status
           FROM vw_rev_service_products
        ), aggregated_rsp1 AS (
         SELECT unique_products.tenant_id,
            unique_products.rev_account_number,
            unique_products.customer_name,
            count(1) AS total_device_count
           FROM unique_products
          GROUP BY unique_products.tenant_id, unique_products.rev_account_number, unique_products.customer_name
        ), aggregated_rsp2 AS (
         SELECT unique_products.tenant_id,
            unique_products.rev_account_number,
            unique_products.customer_name,
            sum(
                CASE
                    WHEN unique_products.is_active_status <> unique_products.rev_is_active_status THEN 1
                    ELSE 0
                END) AS variance_count,
            sum(
                CASE
                    WHEN unique_products.is_active_status IS TRUE OR unique_products.rev_is_active_status IS TRUE THEN 1
                    ELSE 0
                END) AS any_active_count,
            sum(
                CASE
                    WHEN unique_products.rev_is_active_status IS TRUE THEN 1
                    ELSE 0
                END) AS rev_active_count
           FROM unique_products
          GROUP BY unique_products.tenant_id, unique_products.rev_account_number, unique_products.customer_name
        )
 SELECT gen_random_uuid() AS id,
    rsp1a.tenant_id,
    COALESCE(rsp1a.rev_account_number, 'Unassigned'::character varying) AS rev_customer_id,
    COALESCE(rsp1a.customer_name, 'Unassigned'::character varying) AS rev_customer_name,
    rsp2a.rev_active_count AS rev_active_device_count,
    rsp1a.total_device_count AS rev_total_device_count,
    rsp2a.any_active_count AS carrier_total_device_count,
    rsp2a.variance_count
   FROM aggregated_rsp1 rsp1a
     JOIN aggregated_rsp2 rsp2a ON rsp1a.tenant_id = rsp2a.tenant_id AND COALESCE(rsp1a.rev_account_number, ''::character varying)::text = COALESCE(rsp2a.rev_account_number, ''::character varying)::text;



CREATE OR REPLACE VIEW public.vw_rev_assurance_list_view_with_count
 AS
 SELECT DISTINCT vrsp.service_number,
    vrsp.device_status,
    vrsp.carrier_last_status_date,
    vrsp.is_active_status,
    vrsp.should_have_billed_service,
    vrsp.iccid,
    vrsp.service_provider_id,
    vrsp.service_provider,
    vrsp.communication_plan,
    vrsp.activated_date,
    vrsp.disconnected_date,
    vrsp.service_product_id,
    vrsp.package_id,
    vrsp.service_id,
    vrsp.product_id,
    vrsp.description,
    vrsp.rate,
    vrsp.rev_io_status,
    vrsp.rev_is_active_status,
    vrsp.cost,
    vrsp.wholesale_description,
    vrsp.quantity,
    vrsp.integration_authentication_id,
    vrsp.rev_account_number,
    vrsp.customer_name,
    vrsp.rev_product_id,
    vrsp.product_type_id,
    vrsp.product_description,
    vrsp.service_type_id,
    vrsp.tenant_id,
    vrsp.carrier_activated_date,
    vrsp.customer_rate_plan_name,
    vrsp.carrier_rate_plan_name AS carrier_rate_plan,
    vrag.rev_customer_name,
    vrag.rev_active_device_count,
    vrag.rev_total_device_count,
    vrag.carrier_total_device_count,
    vrag.variance_count,
    vrsp.customer_id,
    row_number() OVER (ORDER BY vrsp.customer_id) AS id
   FROM vw_rev_service_products vrsp
     JOIN vw_revenue_assurance_group vrag ON vrsp.rev_account_number::text = vrag.rev_customer_id::text AND vrsp.tenant_id = vrag.tenant_id;

CREATE OR REPLACE VIEW public.vw_rev_assurance_list_view_with_count_variance
 AS
 SELECT vrsp.service_number,
    vrsp.device_status,
    vrsp.carrier_last_status_date,
    vrsp.is_active_status,
    vrsp.should_have_billed_service,
    vrsp.iccid,
    vrsp.service_provider_id,
    vrsp.service_provider,
    vrsp.communication_plan,
    vrsp.activated_date,
    vrsp.disconnected_date,
    vrsp.service_product_id,
    vrsp.package_id,
    vrsp.service_id,
    vrsp.product_id,
    vrsp.description,
    vrsp.rate,
    vrsp.rev_io_status,
    vrsp.rev_is_active_status,
    vrsp.cost,
    vrsp.wholesale_description,
    vrsp.quantity,
    vrsp.integration_authentication_id,
    vrsp.rev_account_number,
    vrsp.customer_name,
    vrsp.rev_product_id,
    vrsp.product_type_id,
    vrsp.product_description,
    vrsp.service_type_id,
    vrsp.tenant_id,
    vrsp.carrier_activated_date,
    vrsp.customer_rate_plan_name,
    vrsp.carrier_rate_plan_name AS carrier_rate_plan,
    vrag.rev_customer_name,
    vrag.rev_active_device_count,
    vrag.rev_total_device_count,
    vrag.carrier_total_device_count,
    vrag.variance_count,
    vrsp.customer_id
   FROM vw_rev_service_products vrsp
     LEFT JOIN vw_revenue_assurance_group vrag ON vrsp.rev_account_number::text = vrag.rev_customer_id::text
  WHERE vrsp.device_status::text <> vrsp.rev_io_status::text;


CREATE OR REPLACE VIEW public.vw_rev_assurance_record_discrepancies
 AS
 WITH filtered_data AS (
         SELECT vw_rev_assurance_list_view_with_count.service_provider,
            vw_rev_assurance_list_view_with_count.device_status,
            vw_rev_assurance_list_view_with_count.rev_io_status,
            vw_rev_assurance_list_view_with_count.carrier_last_status_date
           FROM vw_rev_assurance_list_view_with_count
        )
 SELECT filtered_data.service_provider,
        CASE
            WHEN filtered_data.device_status::text = ''::text THEN concat('Device not in Rev.IO, but activated in carrier.')
            WHEN filtered_data.rev_io_status::text = 'deactivated'::text THEN concat('Device is Deactivated in Rev.IO, but ', filtered_data.device_status, ' in carrier.')
            WHEN filtered_data.device_status::text = 'deactivated'::text THEN concat('Device is Deactivated in carrier, but activated in Rev.IO.')
            ELSE concat('Device is ', filtered_data.device_status, ' in both Rev.IO and carrier.')
        END AS message,
    count(*) AS message_count,
    filtered_data.carrier_last_status_date
   FROM filtered_data
  GROUP BY filtered_data.service_provider, (
        CASE
            WHEN filtered_data.device_status::text = ''::text THEN concat('Device not in Rev.IO, but activated in carrier.')
            WHEN filtered_data.rev_io_status::text = 'deactivated'::text THEN concat('Device is Deactivated in Rev.IO, but ', filtered_data.device_status, ' in carrier.')
            WHEN filtered_data.device_status::text = 'deactivated'::text THEN concat('Device is Deactivated in carrier, but activated in Rev.IO.')
            ELSE concat('Device is ', filtered_data.device_status, ' in both Rev.IO and carrier.')
        END), filtered_data.carrier_last_status_date
  ORDER BY filtered_data.service_provider, (count(*)) DESC;


CREATE OR REPLACE VIEW public.usp_optimization_customer_charges_count_for_cross_optimization
 AS
 SELECT customer_charges_count.instance_id,
    sum(customer_charges_count.charge_count) AS chargecount,
    sum(customer_charges_count.overage_charge_amount) AS overagechargeamount,
    sum(customer_charges_count.total_charge_amount) AS totalchargeamount,
    sum(customer_charges_count.unassigned_device_count) AS unassigneddevicecount,
    sum(customer_charges_count.sms_charge_total) AS smschargetotal,
    row_number() OVER (ORDER BY customer_charges_count.instance_id) AS id
   FROM ( SELECT oq.instance_id,
            sum(
                CASE
                    WHEN jcust_rp.id IS NOT NULL THEN 1
                    ELSE 0
                END) AS charge_count,
            sum(COALESCE(odr.charge_amt::numeric, 0.0) - COALESCE(jcust_rp.base_rate::numeric, 0.0)) AS overage_charge_amount,
            sum(COALESCE(odr.charge_amt::numeric, 0.0)) AS total_charge_amount,
            sum(
                CASE
                    WHEN jcust_rp.id IS NULL THEN 1
                    ELSE 0
                END) AS unassigned_device_count,
            sum(COALESCE(odr.sms_charge_amount::numeric, 0.0)) AS sms_charge_total
           FROM optimization_smi_result odr
             JOIN optimization_queue oq ON odr.queue_id = oq.id
             JOIN optimization_instance oi ON oq.instance_id = oi.id
             JOIN sim_management_inventory smi ON odr.sim_management_inventory_id = smi.id
             LEFT JOIN customerrateplan jcust_rp ON odr.assigned_customer_rate_plan_id = jcust_rp.id
          GROUP BY oq.instance_id) customer_charges_count
  GROUP BY customer_charges_count.instance_id;


CREATE OR REPLACE VIEW public.vw_optimization_instance
 AS
 SELECT row_number() OVER (ORDER BY oi.id) AS id,
    oi.billing_period_start_date,
    oi.billing_period_end_date,
    oi.run_status_id,
    os.display_name AS run_status,
    oi.run_start_time,
    oi.run_end_time,
    rc.rev_customer_id,
    TRIM(BOTH FROM rc.customer_name) AS customer_name,
    od.device_count,
    ocg.total_cost,
    ocg.total_base_rate_amt,
    ocg.total_rate_charge_amt,
    ocg.total_overage_charge_amt,
    oirf.id AS results_id,
    odrrpq.rate_plan_queue_count,
    odrccq.customer_charge_queue_count,
    oi.service_provider_id,
    sp.display_name AS service_provider,
    oi.tenant_id,
    oi.row_uuid,
    oi.optimization_session_id,
    oss.session_id,
    ot.name AS optimization_type,
    oi.amop_customer_id,
    TRIM(BOTH FROM s.customer_name) AS amop_customer_name,
    oi.service_provider_ids,
    ( SELECT string_agg(sp_1.display_name::text, ', '::text) AS string_agg
           FROM unnest(string_to_array(oi.service_provider_ids, ','::text)) ids(ids)
             LEFT JOIN serviceprovider sp_1 ON sp_1.id = ids.ids::integer) AS display_names,
    oi.id AS optimization_instance_id,
    oss.progress,
    oss.optimization_run_time_error
   FROM optimization_instance oi
     JOIN optimization_status os ON oi.run_status_id = os.id
     LEFT JOIN serviceprovider sp ON sp.id = oi.service_provider_id
     JOIN optimization_session oss ON oss.id = oi.optimization_session_id
     JOIN optimization_type ot ON ot.id = oss.optimization_type_id
     LEFT JOIN ( SELECT ocg_1.instance_id,
            sum(oq.total_cost) AS total_cost,
            sum(oq.total_base_rate_amt) AS total_base_rate_amt,
            sum(oq.total_rate_charge_amt) AS total_rate_charge_amt,
            sum(oq.total_overage_charge_amt) AS total_overage_charge_amt
           FROM optimization_comm_group ocg_1
             JOIN ( SELECT oq2.comm_plan_group_id,
                    oq2.total_cost,
                    oq2.total_base_rate_amt,
                    oq2.total_rate_charge_amt,
                    oq2.total_overage_charge_amt
                   FROM ( SELECT optimization_queue.comm_plan_group_id,
                            optimization_queue.total_cost,
                            optimization_queue.total_base_rate_amt,
                            optimization_queue.total_rate_charge_amt,
                            optimization_queue.total_overage_charge_amt,
                            row_number() OVER (PARTITION BY optimization_queue.comm_plan_group_id ORDER BY optimization_queue.total_cost) AS record_number
                           FROM optimization_queue
                          WHERE optimization_queue.total_cost IS NOT NULL) oq2
                  WHERE oq2.record_number = 1) oq ON ocg_1.id = oq.comm_plan_group_id
          GROUP BY ocg_1.instance_id) ocg ON ocg.instance_id = oi.id
     LEFT JOIN ( SELECT device_count_by_type.instance_id,
            sum(device_count_by_type.device_count) AS device_count
           FROM ( SELECT optimization_smi.instance_id,
                    count(1) AS device_count
                   FROM optimization_smi
                  GROUP BY optimization_smi.instance_id) device_count_by_type
          GROUP BY device_count_by_type.instance_id) od ON oi.id = od.instance_id
     LEFT JOIN ( SELECT optimization_instance_result_file.id,
            optimization_instance_result_file.instance_id,
            row_number() OVER (PARTITION BY optimization_instance_result_file.instance_id ORDER BY optimization_instance_result_file.created_date DESC) AS record_number
           FROM optimization_instance_result_file) oirf ON oi.id = oirf.instance_id AND oirf.record_number = 1
     LEFT JOIN revcustomer rc ON rc.id = oi.rev_customer_id
     LEFT JOIN ( SELECT rate_plan_queue_count_by_type.instance_id,
            sum(rate_plan_queue_count_by_type.rate_plan_queue_count) AS rate_plan_queue_count
           FROM ( SELECT oq.instance_id,
                    count(1) AS rate_plan_queue_count
                   FROM optimization_smi_result_rate_plan_queue osrrpq
                     JOIN optimization_smi_result osr ON osr.id = osrrpq.optimization_smi_result_id
                     JOIN optimization_queue oq ON osr.queue_id = oq.id
                  GROUP BY oq.instance_id) rate_plan_queue_count_by_type
          GROUP BY rate_plan_queue_count_by_type.instance_id) odrrpq ON oi.id = odrrpq.instance_id
     LEFT JOIN ( SELECT customer_charge_queue_count_by_type.instance_id,
            sum(customer_charge_queue_count_by_type.customer_charge_queue_count) AS customer_charge_queue_count
           FROM ( SELECT oq4.instance_id,
                    count(1) AS customer_charge_queue_count
                   FROM optimization_smi_result_customer_charge_queue osrccq2
                     JOIN optimization_smi_result osr2 ON osr2.id = osrccq2.optimization_smi_result_id
                     JOIN optimization_queue oq4 ON osr2.queue_id = oq4.id
                  GROUP BY oq4.instance_id) customer_charge_queue_count_by_type
          GROUP BY customer_charge_queue_count_by_type.instance_id) odrccq ON oi.id = odrccq.instance_id
     LEFT JOIN customers s ON oi.amop_customer_id = s.id;


CREATE OR REPLACE VIEW public.vw_optimization_session
 AS
 SELECT os.id,
    os.session_id,
    os.billing_period_start_date,
    os.billing_period_end_date,
    os.tenant_id,
    os.service_provider_id,
    sp.display_name AS serviceprovider,
    os.created_date,
    ot.id AS optimization_type_id,
    ot.name AS optimization_type,
    os.is_active,
    oi2.device_count,
    oi2.total_cost,
    oi2.total_base_rate_amt,
    oi2.total_rate_charge_amt,
    oi2.total_overage_charge_amt,
    oi2.results_count,
    oi2.instance_count,
    oi2.customer_charge_queue_count,
    oi2.completed_instance_count,
    os.service_provider_ids AS serviceproviderids,
    os.customer_id,
    ( SELECT string_agg(sp_1.display_name::text, ', '::text) AS string_agg
           FROM unnest(string_to_array(os.service_provider_ids, ','::text)) ids(ids)
             LEFT JOIN serviceprovider sp_1 ON sp_1.id = ids.ids::integer) AS displaynames
   FROM optimization_session os
     LEFT JOIN serviceprovider sp ON sp.id = os.service_provider_id
     JOIN optimization_type ot ON ot.id = os.optimization_type_id
     LEFT JOIN ( SELECT oi.optimization_session_id,
            sum(COALESCE(ocg.total_cost, 0::numeric)) AS total_cost,
            sum(ocg.total_base_rate_amt) AS total_base_rate_amt,
            sum(ocg.total_rate_charge_amt) AS total_rate_charge_amt,
            sum(ocg.total_overage_charge_amt) AS total_overage_charge_amt,
            sum(COALESCE(od.device_count, 0::bigint)) AS device_count,
            sum(
                CASE
                    WHEN COALESCE(oirf.id, 0::bigint) > 0 THEN 1
                    ELSE 0
                END) AS results_count,
            count(*) AS instance_count,
            sum(odrccq.customer_charge_queue_count) AS customer_charge_queue_count,
            sum(
                CASE
                    WHEN oi.run_status_id >= 6 THEN 1
                    ELSE 0
                END) AS completed_instance_count
           FROM optimization_instance oi
             LEFT JOIN ( SELECT ocg_1.instance_id,
                    sum(oq.total_cost) AS total_cost,
                    sum(oq.total_base_rate_amt) AS total_base_rate_amt,
                    sum(oq.total_rate_charge_amt) AS total_rate_charge_amt,
                    sum(oq.total_overage_charge_amt) AS total_overage_charge_amt
                   FROM optimization_comm_group ocg_1
                     JOIN ( SELECT oq2.comm_plan_group_id,
                            oq2.total_cost,
                            oq2.total_base_rate_amt,
                            oq2.total_rate_charge_amt,
                            oq2.total_overage_charge_amt
                           FROM ( SELECT optimization_queue.comm_plan_group_id,
                                    optimization_queue.total_cost,
                                    optimization_queue.total_base_rate_amt,
                                    optimization_queue.total_rate_charge_amt,
                                    optimization_queue.total_overage_charge_amt,
                                    row_number() OVER (PARTITION BY optimization_queue.comm_plan_group_id ORDER BY optimization_queue.total_cost) AS record_number
                                   FROM optimization_queue
                                  WHERE optimization_queue.total_cost IS NOT NULL) oq2
                          WHERE oq2.record_number = 1) oq ON ocg_1.id = oq.comm_plan_group_id
                  GROUP BY ocg_1.instance_id) ocg ON ocg.instance_id = oi.id
             LEFT JOIN ( SELECT optimization_smi.instance_id,
                    count(1) AS device_count
                   FROM optimization_smi
                  GROUP BY optimization_smi.instance_id) od ON oi.id = od.instance_id
             LEFT JOIN ( SELECT optimization_instance_result_file.id,
                    optimization_instance_result_file.instance_id,
                    row_number() OVER (PARTITION BY optimization_instance_result_file.instance_id ORDER BY optimization_instance_result_file.created_date DESC) AS record_number
                   FROM optimization_instance_result_file) oirf ON oi.id = oirf.instance_id AND oirf.record_number = 1
             LEFT JOIN ( SELECT customer_charge_queue_count_by_type.instance_id,
                    sum(customer_charge_queue_count_by_type.customer_charge_queue_count) AS customer_charge_queue_count
                   FROM ( SELECT oq4.instance_id,
                            count(1) AS customer_charge_queue_count
                           FROM optimization_smi_result_customer_charge_queue osrccq
                             JOIN optimization_smi_result osr ON osr.id = osrccq.optimization_smi_result_id
                             JOIN optimization_queue oq4 ON osr.queue_id = oq4.id
                          GROUP BY oq4.instance_id) customer_charge_queue_count_by_type
                  GROUP BY customer_charge_queue_count_by_type.instance_id) odrccq ON oi.id = odrccq.instance_id
          GROUP BY oi.optimization_session_id) oi2 ON oi2.optimization_session_id = os.id
  WHERE os.created_date < (CURRENT_TIMESTAMP + '35 days'::interval) OR oi2.instance_count > 0;

CREATE OR REPLACE VIEW public.vw_optimization_smi_rate_plan_change_count
 AS
 SELECT oq.instance_id,
    count(1) AS total_device_count,
    sum(
        CASE
            WHEN jd.carrier_rate_plan_id <> COALESCE(odr.assigned_carrier_rate_plan_id, odr.assigned_customer_rate_plan_id) THEN 1
            ELSE 0
        END) AS target_device_count,
    row_number() OVER (ORDER BY (( SELECT NULL::text))) AS id
   FROM optimization_smi_result odr
     JOIN sim_management_inventory jd ON odr.sim_management_inventory_id = jd.id
     JOIN ( SELECT oq_group.id,
            oq_group.instance_id
           FROM ( SELECT optimization_queue.id,
                    optimization_queue.instance_id,
                    row_number() OVER (PARTITION BY optimization_queue.comm_plan_group_id ORDER BY optimization_queue.total_cost) AS record_number
                   FROM optimization_queue
                  WHERE optimization_queue.total_cost IS NOT NULL AND optimization_queue.run_end_time IS NOT NULL) oq_group
          WHERE oq_group.record_number = 1) oq ON odr.queue_id = oq.id
  GROUP BY oq.instance_id;

CREATE OR REPLACE VIEW public.vw_optimization_customer_charges_count
 AS
 SELECT oq.instance_id,
    sum(
        CASE
            WHEN jcust_rp.id IS NOT NULL THEN 1
            ELSE 0
        END) AS chargecount,
    sum(COALESCE(odr.overage_charge_amt, 0.0) + COALESCE(odr.rate_charge_amt, 0.0)) AS overagechargeamount,
    sum(COALESCE(odr.charge_amt, 0.0)) AS totalchargeamount,
    sum(
        CASE
            WHEN jcust_rp.id IS NULL THEN 1
            ELSE 0
        END) AS unassigneddevicecount,
    sum(COALESCE(odr.sms_charge_amount, 0.0)) AS smschargetotal,
    row_number() OVER (ORDER BY oq.instance_id) AS id
   FROM optimization_smi_result odr
     JOIN optimization_queue oq ON odr.queue_id = oq.id
     JOIN optimization_instance oi ON oq.instance_id = oi.id
     JOIN sim_management_inventory smi ON odr.sim_management_inventory_id = smi.id
     LEFT JOIN customerrateplan jcust_rp ON odr.assigned_customer_rate_plan_id = jcust_rp.id
  GROUP BY oq.instance_id;

CREATE OR REPLACE VIEW public.usp_optimization_customer_charges_count_for_cross_optimization
 AS
 SELECT customer_charges_count.instance_id,
    sum(customer_charges_count.charge_count) AS chargecount,
    sum(customer_charges_count.overage_charge_amount) AS overagechargeamount,
    sum(customer_charges_count.total_charge_amount) AS totalchargeamount,
    sum(customer_charges_count.unassigned_device_count) AS unassigneddevicecount,
    sum(customer_charges_count.sms_charge_total) AS smschargetotal,
    row_number() OVER (ORDER BY customer_charges_count.instance_id) AS id
   FROM ( SELECT oq.instance_id,
            sum(
                CASE
                    WHEN jcust_rp.id IS NOT NULL THEN 1
                    ELSE 0
                END) AS charge_count,
            sum(COALESCE(odr.charge_amt::numeric, 0.0) - COALESCE(jcust_rp.base_rate::numeric, 0.0)) AS overage_charge_amount,
            sum(COALESCE(odr.charge_amt::numeric, 0.0)) AS total_charge_amount,
            sum(
                CASE
                    WHEN jcust_rp.id IS NULL THEN 1
                    ELSE 0
                END) AS unassigned_device_count,
            sum(COALESCE(odr.sms_charge_amount::numeric, 0.0)) AS sms_charge_total
           FROM optimization_smi_result odr
             JOIN optimization_queue oq ON odr.queue_id = oq.id
             JOIN optimization_instance oi ON oq.instance_id = oi.id
             JOIN sim_management_inventory smi ON odr.sim_management_inventory_id = smi.id
             LEFT JOIN customerrateplan jcust_rp ON odr.assigned_customer_rate_plan_id = jcust_rp.id
          GROUP BY oq.instance_id) customer_charges_count
  GROUP BY customer_charges_count.instance_id;

CREATE OR REPLACE VIEW public.vw_optimization_instance_summary
 AS
 SELECT voi.id,
    voi.billing_period_start_date,
    voi.billing_period_end_date,
    voi.run_status_id,
    voi.run_status,
    voi.run_start_time,
    voi.run_end_time,
    voi.rev_customer_id,
    voi.customer_name,
    voi.device_count,
    voi.total_cost,
    voi.total_base_rate_amt,
    voi.total_rate_charge_amt,
    voi.total_overage_charge_amt,
    voi.results_id,
    voi.rate_plan_queue_count,
    voi.customer_charge_queue_count,
    voi.service_provider_id,
    voi.service_provider,
    voi.tenant_id,
    voi.row_uuid,
    voi.optimization_session_id,
    voi.session_id,
    voi.optimization_type,
    voi.amop_customer_id,
    voi.amop_customer_name,
    voi.service_provider_ids,
    voi.display_names,
        CASE
            WHEN vos.results_count > 0 AND vos.completed_instance_count = vos.instance_count THEN true
            ELSE false
        END AS download,
        CASE
            WHEN vos.total_cost > 0::numeric AND vos.customer_charge_queue_count >= vos.device_count THEN true
            ELSE false
        END AS info,
        CASE
            WHEN vos.total_cost > 0::numeric AND vos.customer_charge_queue_count <= vos.instance_count::numeric THEN true
            ELSE false
        END AS upload,
    voi.optimization_instance_id AS instance_id,
    osrpcc.total_device_count,
    osrpcc.target_device_count,
    voccc.chargecount AS charge_count,
    voccc.overagechargeamount AS overage_charge_amount,
    voccc.totalchargeamount AS total_charge_amount,
    voccc.unassigneddevicecount AS unassigned_device_count,
    voccc.smschargetotal AS sms_charge_total,
    uocccfco.chargecount AS cross_opt_charge_count,
    uocccfco.overagechargeamount AS cross_opt_overage_charge_amt,
    uocccfco.totalchargeamount AS cross_opt_total_charge_amt,
    uocccfco.unassigneddevicecount AS cross_opt_unassigned_device_count,
    uocccfco.smschargetotal AS cross_opt_sms_charge_total
   FROM vw_optimization_instance voi
     LEFT JOIN vw_optimization_session vos ON voi.session_id = vos.session_id
     LEFT JOIN vw_optimization_smi_rate_plan_change_count osrpcc ON osrpcc.instance_id = voi.optimization_instance_id
     LEFT JOIN vw_optimization_customer_charges_count voccc ON voccc.instance_id = voi.optimization_instance_id
     LEFT JOIN usp_optimization_customer_charges_count_for_cross_optimization uocccfco ON uocccfco.instance_id = voi.optimization_instance_id;

CREATE OR REPLACE VIEW public.vw_optimization_push_charges_data
 AS
 SELECT osmi.iccid,
    vos.service_provider_id,
    vos.service_provider,
    vos.rev_customer_id,
    vos.customer_name,
    osmi.msisdn,
    vos.run_status,
    vos.optimization_instance_id AS instance_id,
    ocp.error_message,
    vos.session_id,
    voccc.totalchargeamount AS total_charge_amount,
    row_number() OVER (ORDER BY (( SELECT NULL::text AS text))) AS id
   FROM vw_optimization_instance vos
     JOIN optimization_smi osmi ON vos.optimization_instance_id = osmi.instance_id
     JOIN vw_optimization_customer_charges_count voccc ON voccc.instance_id = vos.optimization_instance_id
     JOIN optimization_customer_processing ocp ON vos.optimization_session_id = ocp.session_id;

CREATE OR REPLACE VIEW public.vw_optimization_session_running
 AS
 SELECT row_number() OVER (ORDER BY (( SELECT NULL::text AS text))) AS id,
    optimization_session.id AS optimization_session_id,
    optimization_instance.run_status_id AS optimization_instance_status_id,
    optimization_queue.run_status_id AS optimization_queue_status_id,
    optimization_session.service_provider_id
   FROM optimization_session
     LEFT JOIN optimization_instance ON optimization_session.id = optimization_instance.optimization_session_id
     LEFT JOIN optimization_queue ON optimization_instance.id = optimization_queue.instance_id
  WHERE optimization_instance.run_status_id <> 6 OR optimization_instance.run_status_id IS NULL
  GROUP BY optimization_session.id, optimization_instance.run_status_id, optimization_queue.run_status_id, optimization_session.service_provider_id;



CREATE OR REPLACE VIEW public.vw_optimization_sim_card
 AS
 SELECT COALESCE(gen_random_uuid(), gen_random_uuid()) AS id,
    smi.service_provider_id,
    smi.id AS device_id,
    dt.tenant_id,
    device_usage_history.bill_year,
    device_usage_history.bill_month,
    COALESCE(device_usage_history.carrier_cycle_usage::numeric, 0.0) AS carrier_cycle_usage,
    smi.communication_plan,
    smi.msisdn,
    device_usage_history.changed_date AS usage_date,
        CASE
            WHEN device_usage_history.billing_period_id = smi.billing_period_id THEN c2.rate_plan_code
            ELSE device_usage_history.rate_plan_code
        END AS customer_rate_plan_code,
    COALESCE(dt.account_number_integration_authentication_id, r.integration_authentication_id) AS account_number_integration_authentication_id,
    crp.rate_plan_code AS carrier_rate_plan_code,
    device_usage_history.billing_period_id,
    smi.customer_id,
        CASE
            WHEN device_usage_history.billing_period_id = smi.billing_period_id THEN dt.customer_rate_plan_id
            ELSE device_usage_history.customer_rate_plan_id
        END AS customer_rate_plan_id,
        CASE
            WHEN device_usage_history.billing_period_id = smi.billing_period_id THEN dt.customer_data_allocation_mb
            ELSE device_usage_history.customer_data_allocation_mb
        END AS customer_data_allocation_mb,
        CASE
            WHEN device_usage_history.billing_period_id = smi.billing_period_id THEN dt.customer_rate_pool_id
            ELSE device_usage_history.customer_rate_pool_id
        END AS customer_rate_pool_id,
        CASE
            WHEN device_usage_history.billing_period_id = smi.billing_period_id THEN c2.plan_mb
            ELSE device_usage_history.plan_mb
        END AS customer_rate_plan_mb
   FROM sim_management_inventory smi
     JOIN device_tenant dt ON smi.device_id = dt.device_id
     JOIN device_status ds ON smi.device_status_id = ds.id
     JOIN customers c ON c.id = smi.customer_id
     LEFT JOIN revcustomer r ON c.rev_customer_id = r.id
     LEFT JOIN carrier_rate_plan crp ON smi.carrier_rate_plan_id = crp.id
     LEFT JOIN customerrateplan c2 ON dt.customer_rate_plan_id = c2.id
     LEFT JOIN ( SELECT dh.id,
            bp.bill_year,
            bp.bill_month,
            dh.iccid,
            dh.carrier_cycle_usage,
            dh.communication_plan,
            COALESCE(ds_1.status, dh.status) AS status,
            dh.changed_date,
            dh.billing_cycle_end_date,
            COALESCE(sp.bill_period_end_day::character varying(2), '18'::character varying) AS billing_period_end_day,
            COALESCE(sp.bill_period_end_hour::character varying(2), '18'::character varying) AS billing_period_end_hour,
            dh.billing_period_id,
            dh.ctd_sms_usage,
            dh.device_tenant_id,
            ds_1.is_active_status,
            dh.customer_rate_plan_id,
            dh.customer_data_allocation_mb,
            dh.customer_rate_pool_id,
            c3.plan_mb,
            c3.rate_plan_code
           FROM sim_management_inventory_history dh
             JOIN device_status ds_1 ON dh.device_status_id = ds_1.id
             JOIN serviceprovider sp ON sp.id = dh.service_provider_id
             JOIN billing_period bp ON dh.billing_period_id = bp.id
             LEFT JOIN customerrateplan c3 ON dh.customer_rate_plan_id = c3.id
          WHERE dh.is_active = true AND (ds_1.is_active_status = true OR dh.carrier_cycle_usage > 0 AND ds_1.status::text <> 'TEST READY'::text OR dh.last_activated_date IS NOT NULL)) device_usage_history ON smi.device_id = device_usage_history.id AND dt.id = device_usage_history.device_tenant_id
  WHERE smi.is_active = true  AND (ds.is_active_status = true OR device_usage_history.is_active_status = true OR device_usage_history.carrier_cycle_usage > 0 OR smi.last_activated_date IS NOT NULL AND smi.last_activated_date > COALESCE((device_usage_history.billing_cycle_end_date - '1 mon'::interval)::timestamp with time zone, to_timestamp(concat(device_usage_history.bill_year, '/', lpad(device_usage_history.bill_month::text, 2, '0'::text), '/', device_usage_history.billing_period_end_day, ' ', device_usage_history.billing_period_end_hour, ':00'), 'YYYY/MM/DD HH24:MI'::text) - '1 mon'::interval));


CREATE OR REPLACE VIEW public.vw_optimization_smi_result_customer_charge_queue
 AS
 SELECT row_number() OVER (ORDER BY customer_charge_queue.id) AS id,
    optimization_smi_result.queue_id,
    optimization_session.session_id,
    customer_charge_queue.uploaded_file_id,
    optimization_smi_result.usage_mb,
    optimization_smi_result.assigned_customer_rate_plan_id,
    customerrateplan.rate_plan_code,
    customerrateplan.base_rate,
    customerrateplan.surcharge_3g,
    customerrateplan.plan_mb,
    customer_charge_queue.is_processed,
    customer_charge_queue.charge_id,
    customer_charge_queue.charge_amount,
    customer_charge_queue.base_charge_amount,
    customer_charge_queue.total_charge_amount,
    customer_charge_queue.created_date,
    customer_charge_queue.created_by,
    customer_charge_queue.modified_date,
    customer_charge_queue.modified_by,
    customerrateplan.rate_plan_name,
    customerrateplan.overage_rate_cost AS overage_rate_cost_per_mb,
    customerrateplan.rate_charge_amt,
    customerrateplan.display_rate,
    optimization_instance.billing_period_start_date,
    optimization_instance.billing_period_end_date,
    revcustomer.rev_customer_id AS rev_account_number,
    revcustomer.customer_name,
    customerrateplan.data_per_overage_charge,
    customerrateplan.overage_rate_cost,
    customer_charge_queue.has_errors,
    customer_charge_queue.error_message,
    sim_management_inventory.iccid,
    sim_management_inventory.msisdn,
    customer_charge_queue.rev_product_type_id,
    customer_charge_queue.rev_service_number,
    customer_charge_queue.billing_start_date,
    customer_charge_queue.billing_end_date,
    customer_charge_queue.description,
    NULL::text AS cost_center,
    customer_charge_queue.integration_authentication_id,
    integration_authentication.tenant_id,
    customer_charge_queue.sms_rev_product_type_id,
    customer_charge_queue.sms_charge_amount,
    customer_charge_queue.sms_charge_id,
    customerrateplan.sms_rate,
    optimization_smi_result.sms_usage,
    optimization_queue.is_bill_in_advance,
    optimization_instance.optimization_session_id,
    optimization_session.created_date AS session_created_date,
    NULL::integer AS amop_customer_id,
    NULL::character varying AS amop_customer_name,
    customer_charge_queue.rate_charge_amt AS rate_charge_amount,
    customer_charge_queue.overage_charge_amt AS overage_charge_amount,
    customer_charge_queue.base_rate_amt AS base_rate_amount,
    customer_charge_queue.overage_rev_product_type_id,
    sim_management_inventory.service_provider_id,
    customer_charge_queue.rev_product_id,
    customer_charge_queue.sms_rev_product_id,
    customer_charge_queue.overage_rev_product_id,
    optimization_instance.id AS optimization_instance_id,
    COALESCE(EXTRACT(day FROM optimization_instance.billing_period_end_date - optimization_instance.billing_period_start_date), 0::numeric) AS billing_period_duration
   FROM optimization_smi_result_customer_charge_queue customer_charge_queue
     JOIN optimization_smi_result ON customer_charge_queue.optimization_smi_result_id = optimization_smi_result.id
     JOIN sim_management_inventory ON optimization_smi_result.sim_management_inventory_id = sim_management_inventory.id
     LEFT JOIN customerrateplan ON optimization_smi_result.assigned_customer_rate_plan_id = customerrateplan.id
     JOIN optimization_queue ON optimization_smi_result.queue_id = optimization_queue.id
     JOIN optimization_instance ON optimization_queue.instance_id = optimization_instance.id
     JOIN revcustomer ON optimization_instance.rev_customer_id = revcustomer.id
     JOIN optimization_session ON optimization_session.id = optimization_instance.optimization_session_id
     JOIN integration_authentication ON customer_charge_queue.integration_authentication_id = integration_authentication.id OR revcustomer.integration_authentication_id = integration_authentication.id;



CREATE OR REPLACE VIEW public.vw_optimization_smi_result_customer_charge_queue_summary
 AS
 SELECT row_number() OVER (ORDER BY odr_queue.queue_id) AS id,
    odr_queue.queue_id,
    odr_queue.device_count,
    odr_queue.processed_count,
    COALESCE(odr_queue.charge_amount, 0.0) AS charge_amount,
    COALESCE(odr_queue.base_charge_amount, 0.0) AS base_charge_amount,
    COALESCE(odr_queue.total_data_charge_amount, 0.0) AS total_data_charge_amount,
    COALESCE(odr_queue.rate_charge_amt, 0.0) AS rate_charge_amt,
    odr_queue.display_rate,
    oi.billing_period_start_date,
    oi.billing_period_end_date,
    rc.rev_customer_id AS rev_account_number,
    rc.customer_name,
    oi.optimization_session_id,
    NULL::integer AS amop_customer_id,
    NULL::character varying AS amop_customer_name,
        CASE
            WHEN odr_queue.error_count > 0 THEN 'Processed With Errors'::text
            WHEN odr_queue.processed_count = odr_queue.device_count THEN 'Processed'::text
            WHEN odr_queue.processed_count = 0 THEN 'Not Started'::text
            ELSE 'Pending'::text
        END AS charge_status,
    oi.tenant_id,
    odr_queue.sms_charge_amount,
    COALESCE(odr_queue.total_data_charge_amount, 0.0) + COALESCE(odr_queue.sms_charge_amount, 0.0) AS totalchargeamount,
    COALESCE(odr_queue.rate_charge_amount, 0.0) AS rate_charge_amount,
    COALESCE(odr_queue.overage_charge_amount, 0.0) AS overage_charge_amount
   FROM ( SELECT odr.queue_id,
            count(1) AS device_count,
            sum(
                CASE
                    WHEN odr_ccq.is_processed::integer = 1 THEN 1
                    ELSE 0
                END) AS processed_count,
            sum(odr_ccq.charge_amount) AS charge_amount,
            sum(odr_ccq.base_charge_amount) AS base_charge_amount,
            sum(odr_ccq.total_charge_amount) AS total_data_charge_amount,
            sum(jcust_rp.rate_charge_amt::numeric) AS rate_charge_amt,
            sum(jcust_rp.display_rate::numeric) AS display_rate,
            sum(odr_ccq.sms_charge_amount) AS sms_charge_amount,
            sum(
                CASE
                    WHEN odr_ccq.has_errors::integer = 1 THEN 1
                    ELSE 0
                END) AS error_count,
            sum(odr_ccq.rate_charge_amt) AS rate_charge_amount,
            sum(odr_ccq.overage_charge_amt) AS overage_charge_amount
           FROM optimization_smi_result_customer_charge_queue odr_ccq
             JOIN optimization_smi_result odr ON odr_ccq.optimization_smi_result_id = odr.id
             LEFT JOIN customerrateplan jcust_rp ON odr.assigned_customer_rate_plan_id = jcust_rp.id
          GROUP BY odr.queue_id) odr_queue
     JOIN optimization_queue oq ON odr_queue.queue_id = oq.id
     JOIN optimization_instance oi ON oq.instance_id = oi.id
     JOIN revcustomer rc ON oi.rev_customer_id = rc.id;


CREATE OR REPLACE VIEW public.vw_optimization_push_charges_data
 AS
 SELECT osmi.iccid,
    vos.service_provider_id,
    vos.service_provider,
    vos.rev_customer_id,
    vos.customer_name,
    osmi.msisdn,
    vos.run_status,
    vos.optimization_instance_id AS instance_id,
    ocp.error_message,
    vos.session_id,
    voccc.totalchargeamount AS total_charge_amount,
    row_number() OVER (ORDER BY (( SELECT NULL::text AS text))) AS id
   FROM vw_optimization_instance vos
     JOIN optimization_smi osmi ON vos.optimization_instance_id = osmi.instance_id
     JOIN vw_optimization_customer_charges_count voccc ON voccc.instance_id = vos.optimization_instance_id
     JOIN optimization_customer_processing ocp ON vos.optimization_session_id = ocp.session_id;


CREATE OR REPLACE VIEW public.vw_optimization_error_details
 AS
 SELECT vois.rev_customer_id,
    os.iccid,
    os.msisdn,
    vois.customer_name,
    ocp.error_message,
    vois.session_id,
    row_number() OVER (ORDER BY ocp.customer_id) AS id
   FROM vw_optimization_instance_summary vois
     LEFT JOIN optimization_instance oi ON vois.row_uuid::text = oi.row_uuid::text
     LEFT JOIN optimization_smi os ON oi.id = os.instance_id
     LEFT JOIN optimization_customer_processing ocp ON vois.optimization_session_id = ocp.session_id;

CREATE OR REPLACE VIEW public.vw_newly_activated_report
 AS
 SELECT row_number() OVER (ORDER BY sim_management_inventory.id)::integer AS id,
    sim_management_inventory.iccid,
    sim_management_inventory.msisdn,
    sim_management_inventory.billing_account_number,
    sim_management_inventory.carrier_rate_plan_name AS carrier_rate_plan,
    sim_management_inventory.date_activated,
    sim_management_inventory.communication_plan,
    sim_management_inventory.account_number,
    sim_management_inventory.customer_name,
    sim_management_inventory.ip_address,
    sim_management_inventory.service_provider_display_name AS service_provider,
    sim_management_inventory.billing_cycle_start_date,
    sim_management_inventory.billing_cycle_end_date,
    sim_management_inventory.is_active,
    sim_management_inventory.customer_rate_plan_name
   FROM sim_management_inventory
  WHERE sim_management_inventory.is_active = true AND sim_management_inventory.date_activated < (now() - '30 days'::interval);



CREATE OR REPLACE VIEW public.vw_customer_optimization_list_view
 AS
 SELECT voi.id,
    voi.billing_period_start_date,
    voi.billing_period_end_date,
    voi.run_status,
    voi.run_start_time,
    voi.rev_customer_id,
    voi.customer_name,
    voi.device_count,
    voi.total_rate_charge_amt,
    voi.total_overage_charge_amt,
    voi.customer_charge_queue_count,
    voi.service_provider,
    voi.row_uuid,
    voi.session_id,
    voi.optimization_type,
    voi.optimization_instance_id AS instance_id,
    voccc.smschargetotal AS sms_charge_total,
    voi.progress,
    voi.optimization_run_time_error,
    COALESCE(EXTRACT(day FROM voi.billing_period_end_date - voi.billing_period_start_date), 0::numeric) AS billing_period_duration,
    voi.total_overage_charge_amt + voi.total_rate_charge_amt AS total_charges,
    voccc.totalchargeamount AS total_charge_amount,
        CASE
            WHEN voi.session_id IS NOT NULL THEN 'true'::text
            ELSE 'false'::text
        END AS pushed_charges
   FROM vw_optimization_instance voi
     LEFT JOIN vw_optimization_session vos ON voi.session_id = vos.session_id AND voi.optimization_type::text = 'Customer'::text
     LEFT JOIN vw_optimization_customer_charges_count voccc ON voccc.instance_id = voi.optimization_instance_id;

CREATE OR REPLACE VIEW public.vw_carrier_optimization_list_view
 AS
 SELECT voi.id,
    voi.billing_period_start_date,
    voi.billing_period_end_date,
    voi.run_status_id,
    voi.run_status,
    voi.run_start_time,
    voi.run_end_time,
    voi.rev_customer_id,
    voi.customer_name,
    voi.device_count,
    voi.total_cost,
    voi.total_base_rate_amt,
    voi.total_rate_charge_amt,
    voi.total_overage_charge_amt,
    voi.results_id,
    voi.rate_plan_queue_count,
    voi.customer_charge_queue_count,
    voi.service_provider_id,
    voi.service_provider,
    voi.tenant_id,
    voi.row_uuid,
    voi.optimization_session_id,
    voi.session_id,
    voi.optimization_type,
    voi.amop_customer_id,
    voi.amop_customer_name,
    voi.service_provider_ids,
    voi.display_names,
        CASE
            WHEN vos.results_count > 0 AND vos.completed_instance_count = vos.instance_count THEN true
            ELSE false
        END AS download,
        CASE
            WHEN vos.total_cost > 0::numeric AND vos.customer_charge_queue_count >= vos.device_count THEN true
            ELSE false
        END AS info,
        CASE
            WHEN vos.total_cost > 0::numeric AND vos.customer_charge_queue_count <= vos.instance_count::numeric THEN true
            ELSE false
        END AS upload,
    voi.optimization_instance_id AS instance_id,
    osrpcc.total_device_count,
    osrpcc.target_device_count,
    voccc.chargecount AS charge_count,
    voccc.overagechargeamount AS overage_charge_amount,
    voccc.totalchargeamount AS total_charge_amount,
    voccc.unassigneddevicecount AS unassigned_device_count,
    voccc.smschargetotal AS sms_charge_total,
    uocccfco.chargecount AS cross_opt_charge_count,
    uocccfco.overagechargeamount AS cross_opt_overage_charge_amt,
    uocccfco.totalchargeamount AS cross_opt_total_charge_amt,
    uocccfco.unassigneddevicecount AS cross_opt_unassigned_device_count,
    uocccfco.smschargetotal AS cross_opt_sms_charge_total,
    voi.progress,
    voi.optimization_run_time_error,
        CASE
            WHEN voi.session_id IS NOT NULL THEN 'true'::text
            ELSE 'false'::text
        END AS pushed_charges,
    COALESCE(EXTRACT(day FROM voi.billing_period_end_date - voi.billing_period_start_date), 0::numeric) AS billing_period_duration,
    voi.total_overage_charge_amt + voi.total_rate_charge_amt AS total_charges,
    vos.optimization_type_id
   FROM vw_optimization_instance voi
     LEFT JOIN vw_optimization_session vos ON voi.session_id = vos.session_id AND voi.optimization_type::text = 'Carrier'::text
     LEFT JOIN vw_optimization_smi_rate_plan_change_count osrpcc ON osrpcc.instance_id = voi.optimization_instance_id
     LEFT JOIN vw_optimization_customer_charges_count voccc ON voccc.instance_id = voi.optimization_instance_id
     LEFT JOIN usp_optimization_customer_charges_count_for_cross_optimization uocccfco ON uocccfco.instance_id = voi.optimization_instance_id
    WHERE vos.optimization_type_id = 0;


CREATE OR REPLACE VIEW public.vw_optimization_export_device_assignments
 AS
 SELECT oi.id AS instance_id,
    voi.row_uuid,
    voi.session_id,
    voi.rev_customer_id,
    voi.customer_name,
    voi.device_count,
        CASE
            WHEN voi.device_count > 0::numeric THEN oq.total_cost / voi.device_count
            ELSE 0::numeric
        END AS average_cost,
    voi.billing_period_start_date,
    voi.billing_period_end_date,
    voi.billing_period_end_date - voi.billing_period_start_date AS days_in_billing_period,
    oss.iccid,
    oss.cycle_data_usage_mb,
    oss.communication_plan,
    oss.msisdn,
    oss.sms_usage,
    oss.date_activated,
    oss.was_activated_in_this_billing_period,
    oss.days_activated_in_billing_period,
    oss.carrier_rate_plan_id,
    oss.carrier_rate_plan,
    oss.customer_pool_id,
    oss.customer_pool,
    oss.customer_rate_plan_id,
    oss.customer_rate_plan,
    oq.uses_proration,
    oq.total_cost,
    voi.optimization_type,
    voi.service_provider,
    voi.run_status,
    row_number() OVER (ORDER BY oi.id) AS id
   FROM vw_optimization_instance voi
     LEFT JOIN optimization_instance oi ON voi.row_uuid::text = oi.row_uuid::text
     LEFT JOIN ( SELECT os.instance_id,
            os.iccid,
            os.cycle_data_usage_mb,
            os.communication_plan,
            os.msisdn,
            os.sms_usage,
            os.date_activated,
            os.was_activated_in_this_billing_period,
            os.days_activated_in_billing_period,
            smi.carrier_rate_plan_id,
            smi.carrier_rate_plan_name AS carrier_rate_plan,
            smi.customer_rate_pool_id AS customer_pool_id,
            smi.customer_rate_pool_name AS customer_pool,
            smi.customer_rate_plan_id,
            smi.customer_rate_plan_name AS customer_rate_plan
           FROM optimization_smi os
             LEFT JOIN sim_management_inventory smi ON smi.id = os.sim_management_inventory_id
          WHERE smi.is_active IS TRUE) oss ON oss.instance_id = oi.id
     LEFT JOIN optimization_queue oq ON oq.instance_id = oi.id;


CREATE OR REPLACE VIEW public.vw_pool_group_summary_report_with_billing_cycles
 AS
 SELECT du.foundation_account_number,
    du.billing_account_number,
    du.data_group_id,
    du.pool_id,
    du.data_usage,
    du.data_total AS plan_limit_bytes,
        CASE
            WHEN COALESCE(du.data_total, 0::bigint) = 0 THEN NULL::bigint
            ELSE du.data_total - COALESCE(du.data_usage, 0::bigint)
        END AS data_remaining,
        CASE
            WHEN COALESCE(du.data_total, 0::bigint) = 0 THEN NULL::numeric
            ELSE (COALESCE(du.data_usage, 0::bigint)::numeric * 100.0 / du.data_total::numeric)::numeric(18,2)
        END AS data_usage_percentage,
        CASE
            WHEN COALESCE(du.data_total, 0::bigint) = 0 THEN NULL::numeric
            ELSE ((du.data_total - COALESCE(du.data_usage, 0::bigint))::numeric * 100.0 / du.data_total::numeric)::numeric(18,2)
        END AS data_remaining_percentage,
    sp.display_name AS service_provider_name,
        CASE
            WHEN COALESCE(du.pool_id, ''::character varying)::text <> ''::text THEN ( SELECT count(1) AS count
               FROM mobility_device m
              WHERE  m.pool_id::text = du.pool_id::text)
            ELSE 0::bigint
        END AS pool_device_count,
        CASE
            WHEN COALESCE(du.data_group_id, ''::character varying)::text <> ''::text THEN ( SELECT count(1) AS count
               FROM mobility_device m
              WHERE  m.data_group_id::text = du.data_group_id::text)
            ELSE 0::bigint
        END AS data_group_device_count,
    du.created_date,
    row_number() OVER (ORDER BY du.created_date) AS id,
    du.is_active,
    smi.billing_cycle_start_date,
    smi.billing_cycle_end_date
   FROM mobility_device_usage_aggregate du
     JOIN sim_management_inventory smi ON du.billing_account_number::text = smi.billing_account_number::text
     JOIN serviceprovider sp ON du.service_provider_id = sp.id
     JOIN integration i ON sp.integration_id = i.id
  WHERE COALESCE(du.data_group_id, ''::character varying)::text <> ''::text OR COALESCE(du.pool_id, ''::character varying)::text <> ''::text;


CREATE OR REPLACE VIEW public.vw_pool_group_summary_report
 AS
 SELECT du.foundation_account_number,
    du.billing_account_number,
    du.data_group_id,
    du.pool_id,
    du.data_usage,
    du.data_total AS plan_limit_bytes,
        CASE
            WHEN COALESCE(du.data_total, 0::bigint) = 0 THEN NULL::bigint
            ELSE du.data_total - COALESCE(du.data_usage, 0::bigint)
        END AS data_remaining,
        CASE
            WHEN COALESCE(du.data_total, 0::bigint) = 0 THEN NULL::numeric
            ELSE (COALESCE(du.data_usage, 0::bigint)::numeric * 100.0 / du.data_total::numeric)::numeric(18,2)
        END AS data_usage_percentage,
        CASE
            WHEN COALESCE(du.data_total, 0::bigint) = 0 THEN NULL::numeric
            ELSE ((du.data_total - COALESCE(du.data_usage, 0::bigint))::numeric * 100.0 / du.data_total::numeric)::numeric(18,2)
        END AS data_remaining_percentage,
    sp.display_name AS service_provider_name,
        CASE
            WHEN COALESCE(du.pool_id, ''::character varying)::text <> ''::text THEN ( SELECT count(1) AS count
               FROM mobility_device m
              WHERE  m.pool_id::text = du.pool_id::text)
            ELSE 0::bigint
        END AS pool_device_count,
        CASE
            WHEN COALESCE(du.data_group_id, ''::character varying)::text <> ''::text THEN ( SELECT count(1) AS count
               FROM mobility_device m
              WHERE  m.data_group_id::text = du.data_group_id::text)
            ELSE 0::bigint
        END AS data_group_device_count,
    du.created_date,
    row_number() OVER (ORDER BY du.created_date) AS id,
    du.is_active
   FROM mobility_device_usage_aggregate du
     JOIN serviceprovider sp ON du.service_provider_id = sp.id
     JOIN integration i ON sp.integration_id = i.id
  WHERE COALESCE(du.data_group_id, ''::character varying)::text <> ''::text OR COALESCE(du.pool_id, ''::character varying)::text <> ''::text;

CREATE OR REPLACE VIEW public.vw_people_revio_customers
 AS
 WITH distinct_agents AS (
         SELECT DISTINCT ra.id,
            ra.rev_agent_id,
            ra.agent_name
           FROM revagent ra
          WHERE ra.is_active = true
        )
 SELECT row_number() OVER (ORDER BY c.id)::integer AS id,
    c.tenant_name AS partner,
    da.agent_name,
    rc.customer_name AS name,
    rc.rev_customer_id AS account,
    c.customer_bill_period_end_day,
    c.customer_bill_period_end_hour,
    rc.bill_profile_id,
    c.modified_date
   FROM revcustomer rc
     JOIN customers c ON rc.id = c.rev_customer_id
     LEFT JOIN distinct_agents da ON da.rev_agent_id = rc.agent_id
  WHERE (rc.bill_profile_id IN ( SELECT revbillprofile.bill_profile_id
           FROM revbillprofile
          WHERE (revbillprofile.integration_authentication_id IN ( SELECT integration_authentication.id
                   FROM integration_authentication
                  WHERE integration_authentication.tenant_id = 1)) AND revbillprofile.is_active = true )) AND c.is_active = true  AND rc.is_active = true
  ORDER BY c.modified_date DESC;


CREATE OR REPLACE VIEW public.vw_people_revio_customer_list_view
 AS
 SELECT c.id,
    c.tenant_name AS partner,
    ra.agent_name AS agent,
    rc.customer_name AS name,
    rc.rev_customer_id AS account,
    c.customer_bill_period_end_day,
    c.customer_bill_period_end_hour
   FROM revcustomer rc
     LEFT JOIN revagent ra ON ra.rev_agent_id = rc.agent_id
     LEFT JOIN customers c ON c.rev_customer_id = rc.id;


CREATE OR REPLACE VIEW public.vw_people_netsapiens_customers_list_view
 AS
 SELECT extent1.id,
    extent1.tenant_name,
    extent1.customer_name,
    extent1.description,
    extent1.inactivity_start,
    extent1.inactivity_end,
    extent1.apt_suite,
    extent1.address1,
    extent1.address2,
    extent1.city,
    extent1.state,
    extent1.postal_code,
    extent1.country,
    extent1.created_date,
    extent1.created_by,
    extent1.modified_date,
    extent1.modified_by,
    extent1.deleted_date,
    extent1.deleted_by,
    extent1.is_active,
    extent1.netsapiens_type,
    extent1.rev_customer_id,
    extent1.bandwidth_customer_id,
    extent1.netsapiens_customer_id,
    extent1.is_system_default,
    extent1.e911_customer_id,
    extent1.netsapiens_domain_id,
    extent1.customer_rate_plans,
    extent1.customer_bill_period_end_hour,
    extent1.customer_bill_period_end_day
   FROM customers extent1
  WHERE  extent1.is_active = true AND (extent1.netsapiens_customer_id IS NOT NULL OR extent1.netsapiens_domain_id IS NOT NULL)
  ORDER BY extent1.modified_date DESC, extent1.customer_name;


CREATE OR REPLACE VIEW public.vw_people_bandwidth_customers
 AS
 SELECT extent1.id,
    extent1.tenant_name,
    extent1.customer_name,
    extent1.description,
    extent1.inactivity_start,
    extent1.inactivity_end,
    extent1.apt_suite,
    extent1.address1,
    extent1.address2,
    extent1.city,
    bws.bandwidth_account_id,
    extent1.state,
    extent1.postal_code,
    extent1.country,
    extent1.created_date,
    extent1.created_by,
    extent1.modified_date,
    extent1.modified_by,
    extent1.deleted_date,
    extent1.deleted_by,
    extent1.is_active,
    extent1.rev_customer_id,
    extent1.bandwidth_customer_id,
    extent1.netsapiens_customer_id,
    extent1.is_system_default,
    extent1.e911_customer_id,
    extent1.netsapiens_domain_id,
    extent1.customer_rate_plans,
    extent1.customer_bill_period_end_hour,
    extent1.customer_bill_period_end_day
   FROM customers extent1
     JOIN bandwidth_customers bws ON bws.id = extent1.bandwidth_customer_id::double precision::integer
  WHERE  extent1.is_active = true AND (extent1.bandwidth_customer_id IS NOT NULL OR extent1.is_system_default = true)
  ORDER BY extent1.modified_date DESC, extent1.customer_name;

CREATE OR REPLACE VIEW public.vw_automation_rule_log_list_view
 AS
 SELECT autorule.automation_rule_name,
    autorule.service_provider_id,
    serviceprovider.display_name AS service_provider_display_name,
    row_number() OVER (ORDER BY rulelog.id)::integer AS id,
    rulelog.status,
    rulelog.device_updated AS sim,
    td.subscriber_number,
    rulelog.description,
    rulelog.request_body,
    rulelog.response_body,
    rulelog.instance_id,
    rulelog.created_date,
    rulelog.created_by,
    row_number() OVER (PARTITION BY rulelog.instance_id ORDER BY rulelog.created_date) AS step_order
   FROM automation_rule autorule
     JOIN automation_rule_log rulelog ON autorule.id = rulelog.automation_rule_id
     JOIN serviceprovider serviceprovider ON serviceprovider.id = autorule.service_provider_id
     JOIN telegence_device td ON td.iccid::text = rulelog.device_updated;


