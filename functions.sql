CREATE OR REPLACE FUNCTION update_modified_date()
RETURNS TRIGGER AS $$
BEGIN
  NEW.modified_date := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_last_modified_date_()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_modified_date := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_last_modified_date_time()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_modified_date_time := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER update_last_modified
BEFORE UPDATE ON integration
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON serviceprovider
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON bandwidthaccount
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON bandwidth_customers
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON billing_period_status
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON billing_period
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON carrier_rate_plan
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON customer_rate_pool
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON customerrateplan
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON netsapiens_reseller
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON e911customers
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON integration_authentication
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON revcustomer
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON customergroups
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON mobility_feature
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON customer_mobility_feature
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();


CREATE TRIGGER update_last_modified
BEFORE UPDATE ON device_status
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_communication_plan
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON revagent
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON revbillprofile
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_provider
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_service_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_usage_plan_group
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_service
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_inventory
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON service_provider_tenant_configuration
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_bulk_change_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_bulk_change
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_bulk_change_request
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_bulk_change_log
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_carrier_feature_codes_uat
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_product_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_product
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_package
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_status
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON mobility_device_usage_aggregate
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON imei_master
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON sim_management_inventory_history
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON device_status_reason_code
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON netsapiens_device
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_session
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_instance
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_comm_group
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_group
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_group_carrier_rate_plan
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_instance_result_file
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();


CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_queue
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_rate_plan_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_smi_result_customer_charge_queue
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON optimization_smi_result_rate_plan_queue
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON rev_service_product
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();


CREATE TRIGGER update_last_modified
BEFORE UPDATE ON qualification
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON app_file
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_get_usage_by_rate_plan
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_action
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_condition
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_customer_rate_plan
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_followup_effective_date_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_followup
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_detail
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_followup_detail
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_log
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON automation_rule_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON device_status_uploaded_file_detail
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON device_status_uploaded_file
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON e_bonding_device
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON e_bonding_device_sync_audit
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON jasper_device_sync_audit
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON telegence_device
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON telegence_device_mobility_feature
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON telegence_device_sync_audit
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON thing_space_device_sync_audit
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON customer_billing_period
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON imei_type
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();

CREATE TRIGGER update_last_modified
BEFORE UPDATE ON integration_connection
FOR EACH ROW
EXECUTE FUNCTION update_modified_date();







CREATE OR REPLACE FUNCTION public.insert_into_optimization_smi()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.optimization_smi (
        instance_id,
        device_id,
        cycle_data_usage_mb,
        projected_data_usage_mb,
        communication_plan,
        msisdn,
        iccid,
        usage_date,
        created_by,
        created_date,
        amop_device_id,
        service_provider_id,
        date_activated,
        was_activated_in_this_billing_period,
        days_activated_in_billing_period,
        sms_usage,
        optimization_comm_group_id,
        auto_change_rate_plan,
        did
    )
    VALUES (
        NEW.instance_id,
        NEW.device_id,
        NEW.cycle_data_usage_mb,
        NEW.projected_data_usage_mb,
        NEW.communication_plan,
        NEW.msisdn,
        NEW.iccid,
        NEW.usage_date,
        NEW.created_by,
        NEW.created_date,
        NEW.amop_device_id,
        NEW.service_provider_id,
        NEW.date_activated,
        NEW.was_activated_in_this_billing_period,
        NEW.days_activated_in_billing_period,
        NEW.sms_usage,
		NEW.optimization_comm_group_id,
        NEW.auto_change_rate_plan,
        NEW.id
    );
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_into_optimization_smi_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.optimization_smi (
        instance_id,
        device_id,
        cycle_data_usage_mb,
        projected_data_usage_mb,
        communication_plan,
        msisdn,
        iccid,
        usage_date,
        created_by,
        created_date,
        amop_device_id,
        service_provider_id,
        date_activated,
        was_activated_in_this_billing_period,
        days_activated_in_billing_period,
        sms_usage,
		optimization_rate_plan_type_id,
        optimization_group_id,
        auto_change_rate_plan,
		optimization_comm_group_id,
        mid
    )
    VALUES (
        NEW.instance_id,
        NEW.device_id,
        NEW.cycle_data_usage_mb,
        NEW.projected_data_usage_mb,
        NEW.communication_plan,
        NEW.msisdn,
        NEW.iccid,
        NEW.usage_date,
        NEW.created_by,
        NEW.created_date,
        NEW.amop_device_id,
        NEW.service_provider_id,
        NEW.date_activated,
        NEW.was_activated_in_this_billing_period,
        NEW.days_activated_in_billing_period,
        NEW.sms_usage,
        NEW.optimization_rate_plan_type_id,
        NEW.optimization_group_id,
        NEW.auto_change_rate_plan,
		NEW.optimization_comm_group_id,
        NEW.id  -- Use NEW.id to populate the Mid column in optimization_smi
    );
    RETURN NEW;
END;
$BODY$;



CREATE OR REPLACE FUNCTION public.insert_into_optimization_smi_result_from_device_result()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.optimization_smi_result (
        queue_id,
        device_id,
        usage_mb,
        assigned_carrier_rate_plan_id,
        assigned_customer_rate_plan_id,
        customer_rate_pool_id,
        created_by,
        created_date,
        amop_device_id,
        charge_amt,
        billing_period_id,
        sms_usage,
        sms_charge_amount,
        base_rate_amt,
        rate_charge_amt,
        overage_charge_amt,
        did  -- This column will hold the id from optimization_device_result
    )
    VALUES (
        NEW.queue_id,
        NEW.device_id,
        NEW.usage_mb,
        NEW.assigned_carrier_rate_plan_id,
        NEW.assigned_customer_rate_plan_id,
        NEW.customer_rate_pool_id,
        NEW.created_by,
        NEW.created_date,
        NEW.amop_device_id,
        NEW.charge_amt,
        NEW.billing_period_id,
        NEW.sms_usage,
        NEW.sms_charge_amount,
        NEW.base_rate_amt,
        NEW.rate_charge_amt,
        NEW.overage_charge_amt,
        NEW.id  -- Use NEW.id to populate the did column in optimization_smi_result
    );
    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.insert_into_optimization_smi_result_from_mobility_device_result()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.optimization_smi_result (
        queue_id,
        device_id,
        usage_mb,
        assigned_carrier_rate_plan_id,
        assigned_customer_rate_plan_id,
        customer_rate_pool_id,
        created_by,
        created_date,
        amop_device_id,
        charge_amt,
        billing_period_id,
        sms_usage,
        sms_charge_amount,
        base_rate_amt,
        rate_charge_amt,
        overage_charge_amt,
        mid  -- This column will hold the id from optimization_mobility_device_result
    )
    VALUES (
        NEW.queue_id,
        NEW.device_id,
        NEW.usage_mb,
        NEW.assigned_carrier_rate_plan_id,
        NEW.assigned_customer_rate_plan_id,
        NEW.customer_rate_pool_id,
        NEW.created_by,
        NEW.created_date,
        NEW.amop_device_id,
        NEW.charge_amt,
        NEW.billing_period_id,
        NEW.sms_usage,
        NEW.sms_charge_amount,
        NEW.base_rate_amt,
        NEW.rate_charge_amt,
        NEW.overage_charge_amt,
        NEW.id  -- Use NEW.id to populate the did column in optimization_smi_result
    );
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_into_optimization_smi_result_rate_plan_queue_from_device()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN

    INSERT INTO public.optimization_smi_result_rate_plan_queue (
        optimization_device_result_id,
        is_processed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        group_number,
        has_errors,
        error_message,
        did
    )
    VALUES (
        NEW.optimization_device_result_id,
        NEW.is_processed,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.group_number,
        NEW.has_errors,
        NEW.error_message,
        NEW.id
    );

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.insertto_optimization_smi_result_rate_plan_queue_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN

    INSERT INTO public.optimization_smi_result_rate_plan_queue (
        optimization_mobility_device_result_id,
        is_processed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        group_number,
        has_errors,
        error_message,
        mid
    )
    VALUES (
        NEW.optimization_mobility_device_result_id,
        NEW.is_processed,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.group_number,
        NEW.has_errors,
        NEW.error_message,
        NEW.id  -- Insert the `id` from `optimization_mobility_device_result_rate_plan_queue` as `did`
    );

    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_to_optimization_smi_result_customer_chargeq_frm_device()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.optimization_smi_result_customer_charge_queue (
        optimization_device_result_id,
        is_processed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        charge_amount,
        charge_id,
        base_charge_amount,
        total_charge_amount,
        has_errors,
        error_message,
        rev_service_number,
        rev_product_type_id,
        uploaded_file_id,
        billing_start_date,
        billing_end_date,
        description,
        integration_authentication_id,
        billing_period_id,
        sms_rev_product_type_id,
        sms_charge_amount,
        sms_charge_id,
        rate_charge_amt,
        overage_charge_amt,
        base_rate_amt,
        overage_rev_product_type_id,
        rev_product_id,
        sms_rev_product_id,
        overage_rev_product_id,
		did
    )
    VALUES (
        NEW.optimization_device_result_id,
        NEW.is_processed,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.charge_amount,
        NEW.charge_id,
        NEW.base_charge_amount,
        NEW.total_charge_amount,
        NEW.has_errors,
        NEW.error_message,
        NEW.rev_service_number,
        NEW.rev_product_type_id,
        NEW.uploaded_file_id,
        NEW.billing_start_date,
        NEW.billing_end_date,
        NEW.description,
        NEW.integration_authentication_id,
        NEW.billing_period_id,
        NEW.sms_rev_product_type_id,
        NEW.sms_charge_amount,
        NEW.sms_charge_id,
        NEW.rate_charge_amt,
        NEW.overage_charge_amt,
        NEW.base_rate_amt,
        NEW.overage_rev_product_type_id,
        NEW.rev_product_id,
        NEW.sms_rev_product_id,
        NEW.overage_rev_product_id,
		NEW.id
    );

    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_to_optimization_smi_result_cust_chargeq_frm_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.optimization_smi_result_customer_charge_queue (
        optimization_mobility_device_result_id,
        is_processed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        charge_amount,
        charge_id,
        base_charge_amount,
        total_charge_amount,
        has_errors,
        error_message,
        rev_service_number,
        rev_product_type_id,
        uploaded_file_id,
        billing_start_date,
        billing_end_date,
        description,
        integration_authentication_id,
        billing_period_id,
        sms_rev_product_type_id,
        sms_charge_amount,
        sms_charge_id,
        rate_charge_amt,
        overage_charge_amt,
        base_rate_amt,
        overage_rev_product_type_id,
        rev_product_id,
        sms_rev_product_id,
        overage_rev_product_id,
		mid
    )
    VALUES (
        NEW.optimization_mobility_device_result_id,
        NEW.is_processed,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.charge_amount,
        NEW.charge_id,
        NEW.base_charge_amount,
        NEW.total_charge_amount,
        NEW.has_errors,
        NEW.error_message,
        NEW.rev_service_number,
        NEW.rev_product_type_id,
        NEW.uploaded_file_id,
        NEW.billing_start_date,
        NEW.billing_end_date,
        NEW.description,
        NEW.integration_authentication_id,
        NEW.billing_period_id,
        NEW.sms_rev_product_type_id,
        NEW.sms_charge_amount,
        NEW.sms_charge_id,
        NEW.rate_charge_amt,
        NEW.overage_charge_amt,
        NEW.base_rate_amt,
        NEW.overage_rev_product_type_id,
        NEW.rev_product_id,
        NEW.sms_rev_product_id,
        NEW.overage_rev_product_id,
		NEW.id
    );

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_optimization_smi()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi
    SET
        instance_id = NEW.instance_id,
        device_id = NEW.device_id,
        cycle_data_usage_mb = NEW.cycle_data_usage_mb,
        projected_data_usage_mb = NEW.projected_data_usage_mb,
        communication_plan = NEW.communication_plan,
        msisdn = NEW.msisdn,
        iccid = NEW.iccid,
        usage_date = NEW.usage_date,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        amop_device_id = NEW.amop_device_id,
        service_provider_id = NEW.service_provider_id,
        date_activated = NEW.date_activated,
        was_activated_in_this_billing_period = NEW.was_activated_in_this_billing_period,
        days_activated_in_billing_period = NEW.days_activated_in_billing_period,
        sms_usage = NEW.sms_usage,
        auto_change_rate_plan = NEW.auto_change_rate_plan,
		optimization_comm_group_id=NEW.optimization_comm_group_id
    WHERE did = OLD.id;  -- Update the row in optimization_smi where did matches the old id
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.update_optimization_smi_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi
    SET
        instance_id = NEW.instance_id,
        device_id = NEW.device_id,
        cycle_data_usage_mb = NEW.cycle_data_usage_mb,
        projected_data_usage_mb = NEW.projected_data_usage_mb,
        communication_plan = NEW.communication_plan,
        msisdn = NEW.msisdn,
        iccid = NEW.iccid,
        usage_date = NEW.usage_date,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        amop_device_id = NEW.amop_device_id,
        service_provider_id = NEW.service_provider_id,
        date_activated = NEW.date_activated,
        was_activated_in_this_billing_period = NEW.was_activated_in_this_billing_period,
        days_activated_in_billing_period = NEW.days_activated_in_billing_period,
        sms_usage = NEW.sms_usage,
        optimization_rate_plan_type_id = NEW.optimization_rate_plan_type_id,
        optimization_group_id = NEW.optimization_group_id,
        auto_change_rate_plan = NEW.auto_change_rate_plan,
		optimization_comm_group_id=NEW.optimization_comm_group_id
    WHERE mid = OLD.id;  -- Match on the 'did' column in optimization_smi

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_optimization_smi_result_cust_chargeq_frm_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi_result_customer_charge_queue
    SET
        optimization_mobility_device_result_id = NEW.optimization_mobility_device_result_id,
        is_processed = NEW.is_processed,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        charge_amount = NEW.charge_amount,
        charge_id = NEW.charge_id,
        base_charge_amount = NEW.base_charge_amount,
        total_charge_amount = NEW.total_charge_amount,
        has_errors = NEW.has_errors,
        error_message = NEW.error_message,
        rev_service_number = NEW.rev_service_number,
        rev_product_type_id = NEW.rev_product_type_id,
        uploaded_file_id = NEW.uploaded_file_id,
        billing_start_date = NEW.billing_start_date,
        billing_end_date = NEW.billing_end_date,
        description = NEW.description,
        integration_authentication_id = NEW.integration_authentication_id,
        billing_period_id = NEW.billing_period_id,
        sms_rev_product_type_id = NEW.sms_rev_product_type_id,
        sms_charge_amount = NEW.sms_charge_amount,
        sms_charge_id = NEW.sms_charge_id,
        rate_charge_amt = NEW.rate_charge_amt,
        overage_charge_amt = NEW.overage_charge_amt,
        base_rate_amt = NEW.base_rate_amt,
        overage_rev_product_type_id = NEW.overage_rev_product_type_id,
        rev_product_id = NEW.rev_product_id,
        sms_rev_product_id = NEW.sms_rev_product_id,
        overage_rev_product_id = NEW.overage_rev_product_id
    WHERE mid = OLD.id;  -- Match based on the corresponding ID

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_optimization_smi_result_customer_chargeq_frm_device()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi_result_customer_charge_queue
    SET
        optimization_device_result_id = NEW.optimization_device_result_id,
        is_processed = NEW.is_processed,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        charge_amount = NEW.charge_amount,
        charge_id = NEW.charge_id,
        base_charge_amount = NEW.base_charge_amount,
        total_charge_amount = NEW.total_charge_amount,
        has_errors = NEW.has_errors,
        error_message = NEW.error_message,
        rev_service_number = NEW.rev_service_number,
        rev_product_type_id = NEW.rev_product_type_id,
        uploaded_file_id = NEW.uploaded_file_id,
        billing_start_date = NEW.billing_start_date,
        billing_end_date = NEW.billing_end_date,
        description = NEW.description,
        integration_authentication_id = NEW.integration_authentication_id,
        billing_period_id = NEW.billing_period_id,
        sms_rev_product_type_id = NEW.sms_rev_product_type_id,
        sms_charge_amount = NEW.sms_charge_amount,
        sms_charge_id = NEW.sms_charge_id,
        rate_charge_amt = NEW.rate_charge_amt,
        overage_charge_amt = NEW.overage_charge_amt,
        base_rate_amt = NEW.base_rate_amt,
        overage_rev_product_type_id = NEW.overage_rev_product_type_id,
        rev_product_id = NEW.rev_product_id,
        sms_rev_product_id = NEW.sms_rev_product_id,
        overage_rev_product_id = NEW.overage_rev_product_id
    WHERE did = OLD.id;  -- Match the row in optimization_smi_result_customer_charge_queue based on the did

    RETURN NEW;
END;
$BODY$;



CREATE OR REPLACE FUNCTION public.update_optimization_smi_result_from_device_result()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi_result
    SET
        queue_id = NEW.queue_id,
        device_id = NEW.device_id,
        usage_mb = NEW.usage_mb,
        assigned_carrier_rate_plan_id = NEW.assigned_carrier_rate_plan_id,
        assigned_customer_rate_plan_id = NEW.assigned_customer_rate_plan_id,
        customer_rate_pool_id = NEW.customer_rate_pool_id,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        amop_device_id = NEW.amop_device_id,
        charge_amt = NEW.charge_amt,
        billing_period_id = NEW.billing_period_id,
        sms_usage = NEW.sms_usage,
        sms_charge_amount = NEW.sms_charge_amount,
        base_rate_amt = NEW.base_rate_amt,
        rate_charge_amt = NEW.rate_charge_amt,
        overage_charge_amt = NEW.overage_charge_amt
    WHERE did = OLD.id;  -- Match on the 'did' column in optimization_smi_result

    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.update_optimization_smi_result_from_mobility_device_result()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi_result
    SET
        queue_id = NEW.queue_id,
        device_id = NEW.device_id,
        usage_mb = NEW.usage_mb,
        assigned_carrier_rate_plan_id = NEW.assigned_carrier_rate_plan_id,
        assigned_customer_rate_plan_id = NEW.assigned_customer_rate_plan_id,
        customer_rate_pool_id = NEW.customer_rate_pool_id,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        amop_device_id = NEW.amop_device_id,
        charge_amt = NEW.charge_amt,
        billing_period_id = NEW.billing_period_id,
        sms_usage = NEW.sms_usage,
        sms_charge_amount = NEW.sms_charge_amount,
        base_rate_amt = NEW.base_rate_amt,
        rate_charge_amt = NEW.rate_charge_amt,
        overage_charge_amt = NEW.overage_charge_amt
    WHERE mid = OLD.id;  -- Match on the 'did' column in optimization_smi_result

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_optimization_smi_result_rate_plan_queue_frm_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Log the incoming values for debugging
    --RAISE NOTICE 'Trigger fired for update: ID = %, Mobility ID = %', NEW.id, NEW.optimization_mobility_device_result_id;

    UPDATE public.optimization_smi_result_rate_plan_queue
    SET
        optimization_device_result_id = NEW.optimization_mobility_device_result_id,
        is_processed = NEW.is_processed,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        group_number = NEW.group_number,
        has_errors = NEW.has_errors,
        error_message = NEW.error_message
    WHERE mid = OLD.id;  -- Match the row in optimization_smi_result_rate_plan_queue based on the id (did)

    -- Log successful update
    --RAISE NOTICE 'Successfully updated optimization_smi_result_rate_plan_queue for Mobility ID: %', NEW.id;

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_optimization_smi_result_rate_plan_queue_from_device()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.optimization_smi_result_rate_plan_queue
    SET
        optimization_device_result_id = NEW.optimization_device_result_id,
        is_processed = NEW.is_processed,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        group_number = NEW.group_number,
        has_errors = NEW.has_errors,
        error_message = NEW.error_message
    WHERE did = OLD.id;  -- Use OLD.id to reference the row before the update

    RETURN NEW;
END;
$BODY$;



CREATE OR REPLACE FUNCTION public.insert_intosim_managementbulkchangerequestfrommobilitychange()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
INSERT INTO public.sim_management_bulk_change_request (
    bulk_change_id,
    subscriber_number,
    change_request,
    device_id,
    is_processed,
    has_errors,
    status,
    status_details,
    processed_date,
    processed_by,
    created_by,
    modified_by,
    modified_date,
    deleted_by,
    deleted_date,
    is_active,
    iccid,
    additional_step_status,
    additional_step_details,
    ip_address,
    device_change_request_id,
	mobility_id
)
VALUES(
  new.bulk_change_id,
    new.subscriber_number,
    new.change_request,
    new.device_id,
    new.is_processed,
    new.has_errors,
    new.status,
    new.status_details,
    new.processed_date,
    new.processed_by,
    new.created_by,
    new.modified_by,
    new.modified_date,
    new.deleted_by,
    new.deleted_date,
    new.is_active,
    new.iccid,
    new.additional_step_status,
    new.additional_step_details,
    new.ip_address,
    new.device_change_request_id,
	new.id
);
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_intosimmanagementbulkchangerequestfromm2m()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.sim_management_bulk_change_request (
        bulk_change_id,
        iccid,
        msisdn,
        ip_address,
        change_request,
        device_id,
        is_processed,
        has_errors,
        status,
        status_details,
        processed_date,
        processed_by,
        created_by,
        created_date,
        modified_by,
        modified_date,
        deleted_by,
        deleted_date,
        is_active,
        device_change_request_id,
        m2m_id
    )
    VALUES (
        NEW.bulk_change_id,
        NEW.iccid,
        NEW.msisdn,
        NEW.ip_address,
        NEW.change_request,
        NEW.device_id,
        NEW.is_processed,
        NEW.has_errors,
        NEW.status,
        NEW.status_details,
        NEW.processed_date,
        NEW.processed_by,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.deleted_by,
        NEW.deleted_date,
        NEW.is_active,
        NEW.device_change_request_id,
        NEW.id
    );

    RETURN NEW;
END;
$BODY$;




CREATE OR REPLACE FUNCTION public.update_sim_management_bulk_change_request_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.sim_management_bulk_change_request (
        bulk_change_id,
        subscriber_number,
        change_request,
        device_id,
        is_processed,
        has_errors,
        status,
        status_details,
        processed_date,
        processed_by,
        created_by,
        modified_by,
        modified_date,
        deleted_by,
        deleted_date,
        is_active,
        iccid,
        additional_step_status,
        additional_step_details,
        ip_address,
        device_change_request_id,
        mobility_id
    )
    VALUES (
        NEW.bulk_change_id,
        NEW.subscriber_number,
        NEW.change_request,
        NEW.device_id,
        NEW.is_processed,
        NEW.has_errors,
        NEW.status,
        NEW.status_details,
        NEW.processed_date,
        NEW.processed_by,
        NEW.created_by,
        NEW.modified_by,
        NEW.modified_date,
        NEW.deleted_by,
        NEW.deleted_date,
        NEW.is_active,
        NEW.iccid,
        NEW.additional_step_status,
        NEW.additional_step_details,
        NEW.ip_address,
        NEW.device_change_request_id,
        NEW.id
    );

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.insert_inventory_history_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Insert into sim_management_inventory_history from mobility_device_history
    INSERT INTO public.sim_management_inventory_history (
        changed_date,
        id,
        service_provider_id,
        foundation_account_number,
        billing_account_number,
        iccid,
        imsi,
        msisdn,
        imei,
        device_status_id,
        status,
        carrier_rate_plan_id,
        rate_plan,
        last_usage_date,
        carrier_cycle_usage,
        ctd_sms_usage,
        ctd_voice_usage,
        ctd_session_count,
        created_by,
        created_date,
        modified_by,
        modified_date,
        last_activated_date,
        deleted_by,
        deleted_date,
        is_active,
        account_number,
        provider_date_added,
        provider_date_activated,
        old_device_status_id,
        old_ctd_data_usage,
        account_number_integration_authentication_id,
        billing_period_id,
        customer_id,
        single_user_code,
        single_user_code_description,
        service_zip_code,
        data_group_id,
        pool_id,
        device_make,
        device_model,
        contract_status,
        ban_status,
        imei_type_id,
        plan_limit_mb,
        customer_rate_plan_id,
        customer_data_allocation_mb,
        username,
        customer_rate_pool_id,
        mobility_device_tenant_id,
        tenant_id,
        ip_address,
        is_pushed,
        m_device_history_id  -- Add device_history_id from mobility_device_history
    )
    VALUES (
        NEW.changed_date,
        NEW.id,
        NEW.service_provider_id,
        NEW.foundation_account_number,
        NEW.billing_account_number,
        NEW.iccid,
        NEW.imsi,
        NEW.msisdn,
        NEW.imei,
        NEW.device_status_id,
        NEW.status,
        NEW.carrier_rate_plan_id,
        NEW.rate_plan,
        NEW.last_usage_date,
        NEW.carrier_cycle_usage,
        NEW.ctd_sms_usage,
        NEW.ctd_voice_usage,
        NEW.ctd_session_count,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.last_activated_date,
        NEW.deleted_by,
        NEW.deleted_date,
        NEW.is_active,
        NEW.account_number,
        NEW.provider_date_added,
        NEW.provider_date_activated,
        NEW.old_device_status_id,
        NEW.old_ctd_data_usage,
        NEW.account_number_integration_authentication_id,
        NEW.billing_period_id,
        NEW.customer_id,
        NEW.single_user_code,
        NEW.single_user_code_description,
        NEW.service_zip_code,
        NEW.data_group_id,
        NEW.pool_id,
        NEW.device_make,
        NEW.device_model,
        NEW.contract_status,
        NEW.ban_status,
        NEW.imei_type_id,
        NEW.plan_limit_mb,
        NEW.customer_rate_plan_id,
        NEW.customer_data_allocation_mb,
        NEW.username,
        NEW.customer_rate_pool_id,
        NEW.mobility_device_tenant_id,
        NEW.tenant_id,
        NEW.ip_address,
        NEW.is_pushed,
        NEW.device_history_id  -- Insert the device_history_id from mobility_device_history into m_device_history_id
    );

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.insert_into_inventory_history()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Insert the new device history record into sim_management_inventory_history
    INSERT INTO public.sim_management_inventory_history (
        changed_date,
        id,
        d_device_history_id, -- Assign the device_history_id to this field
        service_provider_id,
        iccid,
        imsi,
        msisdn,
        imei,
        device_status_id,
        status,
        carrier_rate_plan_id,
        rate_plan,
        last_usage_date,
        apn,
        carrier_cycle_usage,
        ctd_sms_usage,
        ctd_voice_usage,
        ctd_session_count,
        created_by,
        created_date,
        modified_by,
        modified_date,
        last_activated_date,
        deleted_by,
        deleted_date,
        is_active,
        account_number,
        provider_date_added,
        provider_date_activated,
        old_device_status_id,
        old_ctd_data_usage,
        account_number_integration_authentication_id,
        billing_period_id,
        customer_rate_plan_id,
        customer_rate_pool_id,
        device_tenant_id,
        tenant_id,
        "package",
        billing_cycle_end_date,
        bill_year,
        bill_month,
        overage_limit_reached,
        overage_limit_override,
        cost_center,
        username,
        is_pushed
    )
    VALUES (
        NEW.changed_date,
        NEW.id,
        NEW.device_history_id,  -- Assign the device_history_id from the new record
        NEW.service_provider_id,
        NEW.iccid,
        NEW.imsi,
        NEW.msisdn,
        NEW.imei,
        NEW.device_status_id,
        NEW.status,
        NEW.carrier_rate_plan_id,
        NEW.rate_plan,
        NEW.last_usage_date,
        NEW.apn,
        NEW.carrier_cycle_usage,
        NEW.ctd_sms_usage,
        NEW.ctd_voice_usage,
        NEW.ctd_session_count,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.last_activated_date,
        NEW.deleted_by,
        NEW.deleted_date,
        NEW.is_active,
        NEW.account_number,
        NEW.provider_date_added,
        NEW.provider_date_activated,
        NEW.old_device_status_id,
        NEW.old_ctd_data_usage,
        NEW.account_number_integration_authentication_id,
        NEW.billing_period_id,
        NEW.customer_rate_plan_id,
        NEW.customer_rate_pool_id,
        NEW.device_tenant_id,
        NEW.tenant_id,
        NEW."package",
        NEW.billing_cycle_end_date,
        NEW.bill_year,
        NEW.bill_month,
        NEW.overage_limit_reached,
        NEW.overage_limit_override,
        NEW.cost_center,
        NEW.username,
        NEW.is_pushed
    );

    -- Return the new record
    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_inventory_history()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Update the corresponding record in sim_management_inventory_history
    UPDATE public.sim_management_inventory_history
    SET
        changed_date = NEW.changed_date,
        id = NEW.id,
        service_provider_id = NEW.service_provider_id,
        iccid = NEW.iccid,
        imsi = NEW.imsi,
        msisdn = NEW.msisdn,
        imei = NEW.imei,
        device_status_id = NEW.device_status_id,
        status = NEW.status,
        carrier_rate_plan_id = NEW.carrier_rate_plan_id,
        rate_plan = NEW.rate_plan,
        last_usage_date = NEW.last_usage_date,
        apn = NEW.apn,
        carrier_cycle_usage = NEW.carrier_cycle_usage,
        ctd_sms_usage = NEW.ctd_sms_usage,
        ctd_voice_usage = NEW.ctd_voice_usage,
        ctd_session_count = NEW.ctd_session_count,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        last_activated_date = NEW.last_activated_date,
        deleted_by = NEW.deleted_by,
        deleted_date = NEW.deleted_date,
        is_active = NEW.is_active,
        account_number = NEW.account_number,
        provider_date_added = NEW.provider_date_added,
        provider_date_activated = NEW.provider_date_activated,
        old_device_status_id = NEW.old_device_status_id,
        old_ctd_data_usage = NEW.old_ctd_data_usage,
        account_number_integration_authentication_id = NEW.account_number_integration_authentication_id,
        billing_period_id = NEW.billing_period_id,
        customer_rate_plan_id = NEW.customer_rate_plan_id,
        customer_rate_pool_id = NEW.customer_rate_pool_id,
        device_tenant_id = NEW.device_tenant_id,
        tenant_id = NEW.tenant_id,
        "package" = NEW."package",
        billing_cycle_end_date = NEW.billing_cycle_end_date,
        bill_year = NEW.bill_year,
        bill_month = NEW.bill_month,
        overage_limit_reached = NEW.overage_limit_reached,
        overage_limit_override = NEW.overage_limit_override,
        cost_center = NEW.cost_center,
        username = NEW.username,
        is_pushed = NEW.is_pushed
    WHERE d_device_history_id = OLD.device_history_id;  -- Use the OLD.device_history_id to match the correct row

    -- Return the updated row
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.update_inventory_history_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Update the corresponding record in sim_management_inventory_history
    UPDATE public.sim_management_inventory_history
    SET
        changed_date = NEW.changed_date,
        id = NEW.id,
        service_provider_id = NEW.service_provider_id,
        foundation_account_number = NEW.foundation_account_number,
        billing_account_number = NEW.billing_account_number,
        iccid = NEW.iccid,
        imsi = NEW.imsi,
        msisdn = NEW.msisdn,
        imei = NEW.imei,
        device_status_id = NEW.device_status_id,
        status = NEW.status,
        carrier_rate_plan_id = NEW.carrier_rate_plan_id,
        rate_plan = NEW.rate_plan,
        last_usage_date = NEW.last_usage_date,
        carrier_cycle_usage = NEW.carrier_cycle_usage,
        ctd_sms_usage = NEW.ctd_sms_usage,
        ctd_voice_usage = NEW.ctd_voice_usage,
        ctd_session_count = NEW.ctd_session_count,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        last_activated_date = NEW.last_activated_date,
        deleted_by = NEW.deleted_by,
        deleted_date = NEW.deleted_date,
        is_active = NEW.is_active,
        account_number = NEW.account_number,
        provider_date_added = NEW.provider_date_added,
        provider_date_activated = NEW.provider_date_activated,
        old_device_status_id = NEW.old_device_status_id,
        old_ctd_data_usage = NEW.old_ctd_data_usage,
        account_number_integration_authentication_id = NEW.account_number_integration_authentication_id,
        billing_period_id = NEW.billing_period_id,
        customer_id = NEW.customer_id,
        single_user_code = NEW.single_user_code,
        single_user_code_description = NEW.single_user_code_description,
        service_zip_code = NEW.service_zip_code,
        data_group_id = NEW.data_group_id,
        pool_id = NEW.pool_id,
        device_make = NEW.device_make,
        device_model = NEW.device_model,
        contract_status = NEW.contract_status,
        ban_status = NEW.ban_status,
        imei_type_id = NEW.imei_type_id,
        plan_limit_mb = NEW.plan_limit_mb,
        customer_rate_plan_id = NEW.customer_rate_plan_id,
        customer_data_allocation_mb = NEW.customer_data_allocation_mb,
        username = NEW.username,
        customer_rate_pool_id = NEW.customer_rate_pool_id,
        mobility_device_tenant_id = NEW.mobility_device_tenant_id,
        tenant_id = NEW.tenant_id,
        ip_address = NEW.ip_address,
        is_pushed = NEW.is_pushed
    WHERE m_device_history_id = OLD.device_history_id;  -- Match using OLD.device_history_id

    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_into_sim_management_inventory_test_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$


 BEGIN
    -- Insert data into sim_management_inventory_test
    RAISE NOTICE '1-Starting insert_into_sim_management_inventory_test function for mobility_device_id: %', NEW.id;

    INSERT INTO sim_management_inventory (
        mobility_device_id,
        mdt_id,
        tenant_id,
        service_provider_id,
        service_provider_display_name,
        integration_id,
        billing_account_number,
        foundation_account_number,
        iccid,
        imsi,
        msisdn,
        imei,
        customer_id,
        customer_name,
        parent_customer_id,
        rev_customer_id,
        rev_customer_name,
        rev_parent_customer_id,
        device_status_id,
        sim_status,
        carrier_cycle_usage_bytes,
        carrier_cycle_usage_mb,
        date_added,
        date_activated,
        account_number,
        carrier_rate_plan_id,
        carrier_rate_plan_name,
        customer_cycle_usage_mb,
        customer_rate_pool_id,
        customer_rate_pool_name,
        customer_rate_plan_id,
        customer_rate_plan_name,
        customer_rate_plan_code,
        sms_count,
        minutes_used,
        username,
        is_Active_status,
        ip_address,
        service_zip_code,
        rate_plan_soc,
        rate_plan_soc_description,
        data_group_id,
        pool_id,
        next_bill_cycle_date,
        device_make,
        device_model,
        contract_status,
        ban_status,
        imei_type_id,
        plan_limit_mb,
        customer_data_allocation_mb,
        billing_cycle_start_date,
        billing_cycle_end_date,
        customer_rate_plan_mb,
        customer_rate_plan_allows_sim_pooling,
        carrier_rate_plan_mb,
        telegence_features,
        ebonding_features,
        last_usage_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        last_activated_date,
        deleted_by,
        deleted_date,
        is_active,
        cost_center,
        soc,
        rev_vw_device_status
    )
    WITH cte_device_status_history AS (
         SELECT dsh.date_of_change,
            dsh.mobility_device_id,
            dsh.current_status,
            dsh.previous_status
           FROM device_status_history dsh
          WHERE dsh.date_of_change IS NOT NULL
        ), cte_cancel_to_unknown_status AS (
         SELECT dsh.date_of_change,
            dsh.mobility_device_id,
            dsh.current_status,
            dsh.previous_status
           FROM cte_device_status_history dsh
          WHERE lower(dsh.current_status::text) = 'c'::text AND lower(dsh.previous_status::text) <> 'c'::text
        ), cte_other_to_unknown_status AS (
         SELECT dsh.date_of_change,
            dsh.mobility_device_id,
            dsh.current_status,
            dsh.previous_status
           FROM cte_device_status_history dsh
          WHERE lower(dsh.current_status::text) = 'unknown'::text AND lower(dsh.previous_status::text) <> 'c'::text
        )
    SELECT
        md.id as mobility_device_id,
        mdt.id as mdt_id,
        mdt.tenant_id,
        md.service_provider_id,
        sp.display_name as service_provider_display_name,
        sp.integration_id,
        md.billing_account_number,
        md.foundation_account_number,
        md.iccid,
        md.imsi,
        md.msisdn,
        md.imei,
        mdt.customer_id,
        c.customer_name,
        c.parent_customer_id,
        rc.rev_customer_id,
        rc.customer_name,
        rc.rev_parent_customer_id,
        md.device_status_id,
        ds.display_name as sim_status,
        md.carrier_cycle_usage as carrier_cycle_usage_bytes,
        CASE
            WHEN i.id = 12 THEN round(COALESCE(md.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 3)
            ELSE round(COALESCE(md.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END AS carrier_cycle_usage_mb,
        md.date_added,
        md.date_activated,
        mdt.account_number,
        md.carrier_rate_plan_id,
        COALESCE(crp.friendly_name, md.carrier_rate_plan) AS carrier_rate_plan_name,
        CASE
            WHEN c.customer_bill_period_end_day IS NOT NULL AND c.customer_bill_period_end_hour IS NOT NULL THEN round(COALESCE(vw_mobility_customer_current_cycle_device_usage.customer_cycle_usage_byte, 0.0) / 1024.0 / 1024.0, 3)
            ELSE round(COALESCE(md.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END AS customer_cycle_usage_mb,
        mdt.customer_rate_pool_id,
        cpool.name as customer_rate_pool_name,
        mdt.customer_rate_plan_id,
        custrp.rate_plan_name as customer_rate_plan_name,
        custrp.rate_plan_code as customer_rate_plan_code,
        md.sms_count,
        md.minutes_used,
        md.username,
        ds.is_active_status,
        md.ip_address,
        md.service_zip_code,
        md.Single_User_Code AS rate_plan_soc,
        md.single_user_code_description as rate_plan_soc_description,
        md.data_group_id,
        md.pool_id,
        md.next_bill_cycle_date,
        md.device_make,
        md.device_model,
        md.contract_status,
        md.ban_status,
        md.imei_type_id,
        md.plan_limit_mb,
        mdt.customer_data_allocation_mb,
        bp.billing_cycle_start_date,
        bp.billing_cycle_end_date,
        custrp.plan_mb as customer_rate_plan_mb,
        custrp.allows_sim_pooling as customer_rate_plan_allows_sim_pooling,
        crp.plan_mb as carrier_rate_plan_mb,
        (SELECT string_agg(mf.soc_code::text, ','::text) AS string_agg
            FROM telegence_device_mobility_feature tdmf
            JOIN mobility_feature mf ON mf.id = tdmf.mobility_feature_id
            WHERE td.id IS NOT NULL AND tdmf.is_active = true AND tdmf.telegence_device_id = td.id AND mf.is_active = true AND mf.is_retired = false
        ) AS telegence_feature,
        (SELECT string_agg(mf.soc_code::text, ','::text) AS string_agg
            FROM e_bonding_device_mobility_feature ebdmf
            JOIN mobility_feature mf ON mf.id = ebdmf.mobility_feature_id
            WHERE e_bonding_device.id IS NOT NULL  AND ebdmf.is_active = true AND ebdmf.e_bonding_device_id = e_bonding_device.id  AND mf.is_active = true AND mf.is_retired = false
        ) AS e_bonding_feature,
        md.last_usage_date,
        md.created_by,
        md.created_date,
        md.modified_by,
        md.modified_date,
        md.last_activated_date,
        md.deleted_by,
        md.deleted_date,
        md.is_active,
        md.cost_center_1 as cost_center1,
        md.soc,
        CASE
            WHEN (EXISTS ( SELECT 1
               FROM device_status_history dsh
              WHERE dsh.mobility_device_id = md.id AND dsh.date_of_change IS NOT NULL AND (dsh.current_status::text IN ( SELECT ds.status
                       FROM device_status ds
                      WHERE ds.integration_id = sp.integration_id))
              ORDER BY dsh.date_of_change DESC
             LIMIT 1)) THEN
            CASE
                WHEN (EXISTS ( SELECT 1
                   FROM cte_device_status_history dsh
                  WHERE dsh.mobility_device_id = md.id AND lower(dsh.current_status::text) = 'unknown'::text AND lower(dsh.previous_status::text) = 'c'::text)) AND COALESCE(( SELECT max(dsh.date_of_change) AS max
                   FROM cte_other_to_unknown_status dsh
                  WHERE dsh.mobility_device_id = md.id), '1970-01-01 00:00:00'::timestamp without time zone) < (( SELECT max(dsh.date_of_change) AS max
                   FROM cte_cancel_to_unknown_status dsh
                  WHERE dsh.mobility_device_id = md.id)) THEN 'C'::character varying
                ELSE ds.display_name
            END
            ELSE ds.display_name
        END AS rev_vw_device_status
    FROM mobility_device md
    JOIN mobility_device_tenant mdt ON mdt.mobility_device_id = md.id
    JOIN  serviceprovider sp ON md.service_provider_id = sp.id
    LEFT JOIN  device_status ds ON ds.id = NEW.device_status_id
    LEFT JOIN customers c ON c.id = mdt.customer_id AND c.is_active = true
    LEFT JOIN
	    revcustomer rc ON c.rev_customer_id = rc.id AND rc.is_active = true
	LEFT JOIN
	    integration i ON i.id = sp.integration_id AND i.is_active = true and i.portal_type_id =2
	LEFT JOIN
	    carrier_rate_plan crp ON crp.id = md.carrier_rate_plan_id
	LEFT JOIN customerrateplan custrp ON custrp.id=mdt.customer_rate_plan_id
	LEFT JOIN customer_rate_pool cpool ON cpool.id = mdt.customer_rate_pool_id
	LEFT JOIN billing_period bp ON bp.id = md.billing_period_id
	LEFT JOIN telegence_device td ON td.subscriber_number = md.msisdn AND td.service_provider_id = md.service_provider_id
	LEFT JOIN e_bonding_device ON e_bonding_device.subscriber_number = md.msisdn AND e_bonding_device.service_provider_id = md.service_provider_id
	LEFT JOIN vw_mobility_customer_current_cycle_device_usage ON vw_mobility_customer_current_cycle_device_usage.mobility_device_id = md.id AND vw_mobility_customer_current_cycle_device_usage.tenant_id = mdt.tenant_id

	where mdt.mobility_device_id=new.id;

    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.update_into_sim_management_inventory_test_from_mobility()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$


BEGIN
    RAISE NOTICE 'Starting update_into_sim_management_inventory_test function for mobility_device_id: %', NEW.id;

    -- Log the initial values being updated
    RAISE NOTICE 'Updated Record: id: %, service_provider_id: %, iccid: %, msisdn: %, device_status_id: %',
                 NEW.id, NEW.service_provider_id, NEW.iccid, NEW.msisdn, NEW.device_status_id;

    -- Update data in sim_management_inventory_test
    UPDATE sim_management_inventory
    SET
        mobility_device_id = NEW.id,
        mdt_id = mdt.id,
        tenant_id = mdt.tenant_id,
        service_provider_id = NEW.service_provider_id,
        service_provider_display_name = sp.display_name,
        integration_id = sp.integration_id,
        billing_account_number = NEW.billing_account_number,
        foundation_account_number = NEW.foundation_account_number,
        iccid = NEW.iccid,
        imsi = NEW.imsi,
        msisdn = NEW.msisdn,
        imei = NEW.imei,
        customer_id = mdt.customer_id,
        customer_name = c.customer_name,
        parent_customer_id = c.parent_customer_id,
        rev_customer_id = rc.rev_customer_id,
        rev_customer_name = rc.customer_name,
        rev_parent_customer_id = rc.rev_parent_customer_id,
        device_status_id = NEW.device_status_id,
        sim_status = ds.display_name,
        carrier_cycle_usage_bytes = NEW.carrier_cycle_usage,
        carrier_cycle_usage_mb = CASE
            WHEN i.id = 12 THEN round(COALESCE(NEW.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 3)
            ELSE round(COALESCE(NEW.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END,
        date_added = NEW.date_added,
        date_activated = NEW.date_activated,
        account_number = mdt.account_number,
        carrier_rate_plan_id = NEW.carrier_rate_plan_id,
        carrier_rate_plan_name = COALESCE(crp.friendly_name, NEW.carrier_rate_plan),
        customer_cycle_usage_mb = CASE
            WHEN c.customer_bill_period_end_day IS NOT NULL AND c.customer_bill_period_end_hour IS NOT NULL THEN round(COALESCE(vw_mobility_customer_current_cycle_device_usage.customer_cycle_usage_byte, 0.0) / 1024.0 / 1024.0, 3)
            ELSE round(COALESCE(NEW.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END,
        customer_rate_pool_name = cpool.name,
        customer_rate_plan_name = custrp.rate_plan_name,
        customer_rate_plan_code = custrp.rate_plan_code,
        sms_count = NEW.sms_count,
        minutes_used = NEW.minutes_used,
        username = NEW.username,
        is_Active_status = ds.is_active_status,
        ip_address = NEW.ip_address,
        service_zip_code = NEW.service_zip_code,
        rate_plan_soc = NEW.Single_User_Code,
        rate_plan_soc_description = NEW.single_user_code_description,
        data_group_id = NEW.data_group_id,
        pool_id = NEW.pool_id,
        next_bill_cycle_date = NEW.next_bill_cycle_date,
        device_make = NEW.device_make,
        device_model = NEW.device_model,
        contract_status = NEW.contract_status,
        ban_status = NEW.ban_status,
        imei_type_id = NEW.imei_type_id,
        plan_limit_mb = NEW.plan_limit_mb,
        customer_data_allocation_mb = mdt.customer_data_allocation_mb,
        billing_cycle_start_date = bp.billing_cycle_start_date,
        billing_cycle_end_date = bp.billing_cycle_end_date,
        customer_rate_plan_mb = custrp.plan_mb,
        customer_rate_plan_allows_sim_pooling = custrp.allows_sim_pooling,
        carrier_rate_plan_mb = crp.plan_mb,
        telegence_features = (SELECT string_agg(mf.soc_code::text, ','::text) FROM telegence_device_mobility_feature tdmf
            JOIN mobility_feature mf ON mf.id = tdmf.mobility_feature_id
            WHERE tdmf.is_active = true AND tdmf.telegence_device_id = td.id  AND mf.is_active = true AND mf.is_retired = false),
        ebonding_features = (SELECT string_agg(mf.soc_code::text, ','::text) FROM e_bonding_device_mobility_feature ebdmf
            JOIN mobility_feature mf ON mf.id = ebdmf.mobility_feature_id
            WHERE ebdmf.is_active = true AND ebdmf.e_bonding_device_id = e_bonding_device.id AND mf.is_active = true AND mf.is_retired = false),
        last_usage_date = NEW.last_usage_date,
        created_by = NEW.created_by,
        created_date = NEW.created_date,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        last_activated_date = NEW.last_activated_date,
        deleted_by = NEW.deleted_by,
        deleted_date = NEW.deleted_date,
        is_active = NEW.is_active,
        cost_center = NEW.cost_center_1,
		soc=new.soc,
		delta_ctd_data_usage=NEW.delta_ctd_data_usage,
		billing_period_id=NEW.billing_period_id,
		ctd_sms_usage=NEW.ctd_sms_usage,
		ctd_session_count=NEW.ctd_session_count,
		ctd_voice_usage=NEW.ctd_voice_usage,
		rev_service_id=mdt.rev_service_id,
		account_number_integration_authentication_id=mdt.account_number_integration_authentication_id,
		technology_type=NEW.technology_type,
		optimization_group_id=NEW.optimization_group_id
    FROM
        mobility_device md
    JOIN mobility_device_tenant mdt ON mdt.mobility_device_id = md.id
    JOIN serviceprovider sp ON md.service_provider_id = sp.id
    LEFT JOIN device_status ds ON ds.id = md.device_status_id
    LEFT JOIN customers c ON c.id = mdt.customer_id AND c.is_active = true
    LEFT JOIN revcustomer rc ON c.rev_customer_id = rc.id AND rc.is_active = true
    LEFT JOIN integration i ON i.id = sp.integration_id AND i.is_active = true AND  i.portal_type_id = 2
    LEFT JOIN carrier_rate_plan crp ON crp.id = md.carrier_rate_plan_id
    LEFT JOIN customerrateplan custrp ON custrp.id = mdt.customer_rate_plan_id
    LEFT JOIN customer_rate_pool cpool ON cpool.id = mdt.customer_rate_pool_id
    LEFT JOIN billing_period bp ON bp.id = md.billing_period_id
    LEFT JOIN telegence_device td ON td.subscriber_number = md.msisdn AND td.service_provider_id = md.service_provider_id
    LEFT JOIN e_bonding_device ON e_bonding_device.subscriber_number = NEW.msisdn AND e_bonding_device.service_provider_id = md.service_provider_id
    LEFT JOIN vw_mobility_customer_current_cycle_device_usage ON vw_mobility_customer_current_cycle_device_usage.mobility_device_id = md.id AND vw_mobility_customer_current_cycle_device_usage.tenant_id = mdt.tenant_id
    where mdt.mobility_device_id=new.id and sim_management_inventory.mobility_device_id=old.id;

    RAISE NOTICE 'Record updated in sim_management_inventory_test for mobility_device_id: %', NEW.id;
    RETURN NEW;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error updating in sim_management_inventory_test: %', SQLERRM;
        RETURN NULL;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.insert_into_smi_test_from_device()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$

BEGIN
insert into public.sim_management_inventory(
   last_usage_date,
   dt_id,
   device_id,
   service_provider_id,
   service_provider_display_name,
   integration_id,
   iccid,
   msisdn,
   imei,
   eid,
   carrier_cycle_usage_bytes,
   carrier_rate_plan_name,
   carrier_cycle_usage_mb,
   date_added,
   date_activated,
   created_by,
   created_date,
   modified_by,
   modified_date,
   deleted_by,
   deleted_date,
   is_active,
   account_number,
   cost_center,
   username,
   carrier_rate_plan_id,
   device_status_id,
   sim_status,
   is_active_status,
   tenant_id,
   rev_customer_id,
   rev_customer_name,
   rev_parent_customer_id,
   customer_id,
   parent_customer_id,
   customer_name,
   customer_rate_plan_id,
   customer_rate_plan_code,
   customer_rate_plan_name,
   sms_count,
   communication_plan,
   ip_address,
   customer_rate_pool_id,
   customer_rate_pool_name,
   customer_cycle_usage_mb,
   carrier_rate_plan_mb,
   soc,
   billing_cycle_start_date,
   billing_cycle_end_date,
   billing_period_id,
	ctd_sms_usage,
	ctd_session_count,
	package,
	overage_limit_reached,
	overage_limit_override,
	device_description,
    ctd_voice_usage,
	rev_service_id,
	account_number_integration_authentication_id,
	customer_data_allocation_mb,
customer_rate_plan_mb,
customer_rate_plan_allows_sim_pooling

)
select smi.last_usage_date,
       dt.id as dt_id,
	   smi.id as device_id,
	   smi.service_provider_id,
	   sp.display_name as service_provider_display_name,
	   sp.integration_id,
	   smi.iccid,
	   smi.msisdn,
	   smi.imei,
	   smi.eid,
	   smi.carrier_cycle_usage as carrier_cycle_usage_bytes,
	   COALESCE(carrier_rate_plan.friendly_name, smi.carrier_rate_plan) AS carrier_rate_plan_name,
        CASE
            WHEN i.id = 12 THEN round(COALESCE(smi.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 3)
            ELSE round(COALESCE(smi.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END AS carrier_cycle_usage_mb,
	   COALESCE(date_added.date_of_change, smi.date_added) AS date_added,
       COALESCE(date_activated.date_of_change, smi.date_activated) AS date_activated,
	   smi.created_by,
       smi.created_date,
       smi.modified_by,
       smi.modified_date,
       smi.deleted_by,
       smi.deleted_date,
       smi.is_active,
	   dt.account_number,
	   smi.cost_center,
       smi.username,
	   smi.carrier_rate_plan_id,
	   smi.device_status_id,
	   ds.display_name as sim_status,
       ds.is_active_status,
       dt.tenant_id,
       revcust.rev_customer_id,
       revcust.customer_name as rev_customer_name,
       revcust.rev_parent_customer_id,
       cust.id as customer_id,
       cust.parent_customer_id,
       cust.customer_name,
       dt.customer_rate_plan_id,
       customerrateplan.rate_plan_code AS customer_rate_plan_code,
       customerrateplan.rate_plan_name customer_rate_plan_name,
       smi.ctd_sms_usage as sms_count,
       smi.communication_plan,
       smi.ip_address,
       dt.customer_rate_pool_id,
       customer_rate_pool.name AS customer_rate_pool_name,
	   CASE
            WHEN cust.customer_bill_period_end_day IS NOT NULL AND cust.customer_bill_period_end_hour IS NOT NULL THEN convertbytestombbyintegrationid(COALESCE(vw_m2m_customer_current_cycle_device_usage.customer_cycle_usage_byte, 0.0)::bigint, sp.integration_id)
            ELSE convertbytestombbyintegrationid(COALESCE(smi.carrier_cycle_usage::numeric, 0.0)::bigint, sp.integration_id)
        END AS customer_cycle_usage_mb,
        carrier_rate_plan.plan_mb as carrier_rate_plan_mb,
		smi.soc,
		bp.billing_cycle_start_date,
        bp.billing_cycle_end_date,
		smi.billing_period_id,
		smi.ctd_sms_usage,
		smi.ctd_session_count,
		smi.package,
		smi.overage_limit_reached,
		smi.overage_limit_override,
		smi.device_description,
		smi.ctd_voice_usage,
		dt.rev_service_id,
		dt.account_number_integration_authentication_id,
		dt.customer_data_allocation_mb,
        customerrateplan.plan_mb as customer_rate_plan_mb,
        customerrateplan.allows_sim_pooling as customer_rate_plan_allows_sim_pooling
		FROM device smi
		JOIN device_tenant dt ON smi.id = dt.device_id
     JOIN serviceprovider sp ON smi.service_provider_id = sp.id
     LEFT JOIN device_status ds ON ds.id = smi.device_status_id
	 LEFT JOIN billing_period bp ON bp.id = smi.billing_period_id
     LEFT JOIN carrier_rate_plan carrier_rate_plan ON smi.carrier_rate_plan_id = carrier_rate_plan.id
     LEFT JOIN customers cust ON cust.id = dt.customer_id AND cust.is_active = true
     LEFT JOIN revcustomer revcust ON cust.rev_customer_id = revcust.id AND cust.is_active = true
     LEFT JOIN integration i ON i.id = sp.integration_id AND i.is_active = true  AND i.portal_type_id = 0
     LEFT JOIN customerrateplan customerrateplan ON dt.customer_rate_plan_id = customerrateplan.id
     LEFT JOIN customer_rate_pool customer_rate_pool ON dt.customer_rate_pool_id = customer_rate_pool.id
     LEFT JOIN vw_m2m_customer_current_cycle_device_usage ON vw_m2m_customer_current_cycle_device_usage.m2m_device_id = smi.id AND vw_m2m_customer_current_cycle_device_usage.tenant_id = dt.tenant_id
     LEFT JOIN LATERAL ( SELECT device_status_history.date_of_change
           FROM device_status_history
          WHERE device_status_history.iccid::text = smi.iccid::text AND (device_status_history.current_status::text IN ( SELECT device_status.status
                   FROM device_status
                  WHERE sp.integration_id = ds.integration_id AND ds.is_active_status = true))
          ORDER BY device_status_history.date_of_change
         LIMIT 1) date_added ON true
     LEFT JOIN LATERAL ( SELECT device_status_history.date_of_change
           FROM device_status_history
          WHERE device_status_history.iccid::text = smi.iccid::text AND (device_status_history.current_status::text IN ( SELECT device_status.status
                   FROM device_status
                  WHERE sp.integration_id = ds.integration_id))
          ORDER BY device_status_history.date_of_change DESC
         LIMIT 1) date_activated ON true
	where dt.device_id=new.id;

 RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_smi_test_from_device()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$

BEGIN
     RAISE NOTICE 'Old value: %, New value: %,New value: %', OLD.iccid, NEW.iccid,NEW.id;
    UPDATE public.sim_management_inventory
    SET
        last_usage_date = new.last_usage_date,
        dt_id = dt.id,
        device_id = new.id,
        service_provider_id = new.service_provider_id,
        service_provider_display_name = sp.display_name,
        integration_id = sp.integration_id,
        iccid = new.iccid,
        msisdn = new.msisdn,
        imei = new.imei,
        eid = new.eid,
        carrier_cycle_usage_bytes = new.carrier_cycle_usage,
        carrier_rate_plan_name = COALESCE(carrier_rate_plan.friendly_name, new.carrier_rate_plan),
        carrier_cycle_usage_mb = CASE
            WHEN i.id = 12 THEN round(COALESCE(new.carrier_cycle_usage::numeric, 0.0) / 1000.0 / 1000.0, 3)
            ELSE round(COALESCE(new.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END,
        date_added = COALESCE(date_added.date_of_change, new.date_added),
        date_activated = COALESCE(date_activated.date_of_change, new.date_activated),
        created_by = new.created_by,
        created_date = new.created_date,
        modified_by = new.modified_by,
        modified_date = new.modified_date,
        deleted_by = new.deleted_by,
        deleted_date = new.deleted_date,
        is_active = new.is_active,
        account_number = dt.account_number,
        cost_center = new.cost_center,
        username = new.username,
        carrier_rate_plan_id = new.carrier_rate_plan_id,
        device_status_id = new.device_status_id,
        sim_status = ds.display_name,
        is_active_status = ds.is_active_status,
        tenant_id = dt.tenant_id,
        rev_customer_id = revcust.rev_customer_id,
        rev_customer_name = revcust.customer_name,
        rev_parent_customer_id = revcust.rev_parent_customer_id,
        customer_id = cust.id,
        parent_customer_id = cust.parent_customer_id,
        customer_name = cust.customer_name,
        customer_rate_plan_id = dt.customer_rate_plan_id,
        customer_rate_plan_code = customerrateplan.rate_plan_code,
        customer_rate_plan_name = customerrateplan.rate_plan_name,
        sms_count = new.ctd_sms_usage,
        communication_plan = new.communication_plan,
        ip_address = new.ip_address,
        customer_rate_pool_id = dt.customer_rate_pool_id,
        customer_rate_pool_name = customer_rate_pool.name,
        customer_cycle_usage_mb = CASE
            WHEN cust.customer_bill_period_end_day IS NOT NULL AND cust.customer_bill_period_end_hour IS NOT NULL THEN
                convertbytestombbyintegrationid(COALESCE(vw_m2m_customer_current_cycle_device_usage.customer_cycle_usage_byte, 0.0)::bigint, sp.integration_id)
            ELSE
                convertbytestombbyintegrationid(COALESCE(new.carrier_cycle_usage::numeric, 0.0)::bigint, sp.integration_id)
        END,
        carrier_rate_plan_mb = carrier_rate_plan.plan_mb,
        soc = new.soc,
		billing_cycle_start_date=bp.billing_cycle_start_date,
        billing_cycle_end_date=bp.billing_cycle_end_date,
   billing_period_id=NEW.billing_period_id,
	ctd_sms_usage=NEW.ctd_sms_usage,
	ctd_session_count=NEW.ctd_session_count,
	package=NEW.package,
	overage_limit_reached=NEW.overage_limit_reached,
	overage_limit_override=NEW.overage_limit_override,
	device_description=NEW.device_description,
    ctd_voice_usage=NEW.ctd_voice_usage,
	rev_service_id=dt.rev_service_id,
	account_number_integration_authentication_id=dt.account_number_integration_authentication_id,
	customer_data_allocation_mb=dt.customer_data_allocation_mb,
      customer_rate_plan_mb=customerrateplan.plan_mb,
customer_rate_plan_allows_sim_pooling=customerrateplan.allows_sim_pooling
    FROM device smi
    JOIN device_tenant dt ON smi.id = dt.device_id
    JOIN serviceprovider sp ON smi.service_provider_id = sp.id
    LEFT JOIN device_status ds ON ds.id = smi.device_status_id
	 LEFT JOIN billing_period bp ON bp.id = smi.billing_period_id
    LEFT JOIN carrier_rate_plan carrier_rate_plan ON smi.carrier_rate_plan_id = carrier_rate_plan.id
    LEFT JOIN customers cust ON cust.id = dt.customer_id AND cust.is_active = true
    LEFT JOIN revcustomer revcust ON cust.rev_customer_id = revcust.id AND cust.is_active = true
    LEFT JOIN integration i ON i.id = sp.integration_id AND i.is_active = true  AND i.portal_type_id = 0
    LEFT JOIN customerrateplan customerrateplan ON dt.customer_rate_plan_id = customerrateplan.id
    LEFT JOIN customer_rate_pool customer_rate_pool ON dt.customer_rate_pool_id = customer_rate_pool.id
    LEFT JOIN vw_m2m_customer_current_cycle_device_usage ON vw_m2m_customer_current_cycle_device_usage.m2m_device_id = smi.id AND vw_m2m_customer_current_cycle_device_usage.tenant_id = dt.tenant_id
    LEFT JOIN LATERAL (
        SELECT device_status_history.date_of_change
        FROM device_status_history
        WHERE device_status_history.iccid::text = smi.iccid::text
        AND (device_status_history.current_status::text IN (
            SELECT device_status.status
            FROM device_status
            WHERE sp.integration_id = ds.integration_id AND ds.is_active_status = true
        ))
        ORDER BY device_status_history.date_of_change
        LIMIT 1
    ) date_added ON true
    LEFT JOIN LATERAL (
        SELECT device_status_history.date_of_change
        FROM device_status_history
        WHERE device_status_history.iccid::text = smi.iccid::text
        AND (device_status_history.current_status::text IN (
            SELECT device_status.status
            FROM device_status
            WHERE sp.integration_id = ds.integration_id
        ))
        ORDER BY device_status_history.date_of_change DESC
        LIMIT 1
    ) date_activated ON true
    WHERE dt.device_id=new.id and sim_management_inventory.device_id=old.id
	and dt.id=sim_management_inventory.dt_id;

    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.update_smi_frm_device_tenant()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$

BEGIN

RAISE NOTICE 'Before update for device_tenant with dt_id: %, device_id: %', OLD.id, OLD.device_id;

Update public.sim_management_inventory
set tenant_id=new.tenant_id,
     rev_service_id=new.rev_service_id,
	 customer_rate_plan_id=new.customer_rate_plan_id,
	 customer_data_allocation_mb=new.customer_data_allocation_mb,
	 customer_rate_pool_id=new.customer_rate_pool_id,
	 account_number=new.account_number,
	 account_number_integration_authentication_id=new.account_number_integration_authentication_id,
	 customer_id=new.customer_id ,
     parent_customer_id=cust.parent_customer_id,
     customer_name=new.customer_name,
	 customer_rate_plan_code=customerrateplan.rate_plan_code,
     customer_rate_plan_name=customerrateplan.rate_plan_name,
	 customer_rate_pool_name=customer_rate_pool.name,
	 customer_cycle_usage_mb=CASE
            WHEN cust.customer_bill_period_end_day IS NOT NULL AND cust.customer_bill_period_end_hour IS NOT NULL THEN convertbytestombbyintegrationid(COALESCE(vw_m2m_customer_current_cycle_device_usage.customer_cycle_usage_byte, 0.0)::bigint, sp.integration_id)
            ELSE convertbytestombbyintegrationid(COALESCE(smi.carrier_cycle_usage::numeric, 0.0)::bigint, sp.integration_id)
        END,
    customer_rate_plan_mb =customerrateplan.plan_mb,
    customer_rate_plan_allows_sim_pooling=customerrateplan.allows_sim_pooling
	 from device_tenant dt
	 JOIN device smi on NEW.device_id=smi.id
	 JOIN serviceprovider sp ON smi.service_provider_id = sp.id
	 JOIN customers cust ON cust.id = NEW.customer_id AND cust.is_active = true
	 LEFT JOIN customerrateplan customerrateplan ON NEW.customer_rate_plan_id = customerrateplan.id
     LEFT JOIN customer_rate_pool customer_rate_pool ON NEW.customer_rate_pool_id = customer_rate_pool.id
	 LEFT JOIN vw_m2m_customer_current_cycle_device_usage ON vw_m2m_customer_current_cycle_device_usage.m2m_device_id = smi.id AND vw_m2m_customer_current_cycle_device_usage.tenant_id = NEW.tenant_id
 WHERE sim_management_inventory.dt_id = OLD.id
      AND sim_management_inventory.device_id = OLD.device_id;

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_smi_frm_mobility_device_tenant()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$

BEGIN
Update public.sim_management_inventory
set tenant_id=new.tenant_id,
     rev_service_id=new.rev_service_id,
	 customer_rate_plan_id=new.customer_rate_plan_id,
	 customer_data_allocation_mb=new.customer_data_allocation_mb,
	 customer_rate_pool_id=new.customer_rate_pool_id,
	 account_number=new.account_number,
	 account_number_integration_authentication_id=new.account_number_integration_authentication_id,
	 customer_id=NEW.customer_id ,
     parent_customer_id=cust.parent_customer_id,
     customer_name=NEW.customer_name,
	 customer_rate_plan_code=customerrateplan.rate_plan_code,
     customer_rate_plan_name=customerrateplan.rate_plan_name,
	 customer_rate_pool_name=customer_rate_pool.name,

	 customer_cycle_usage_mb=CASE
            WHEN cust.customer_bill_period_end_day IS NOT NULL AND cust.customer_bill_period_end_hour IS NOT NULL THEN round(COALESCE(vw_mobility_customer_current_cycle_device_usage.customer_cycle_usage_byte, 0.0) / 1024.0 / 1024.0, 3)
            ELSE round(COALESCE(smi.carrier_cycle_usage::numeric, 0.0) / 1024.0 / 1024.0, 3)
        END,
	customer_rate_plan_mb=customerrateplan.plan_mb,
	customer_rate_plan_allows_sim_pooling=customerrateplan.allows_sim_pooling
	 from mobility_device_tenant mdt
	 JOIN mobility_device smi on NEW.mobility_device_id=smi.id
	 LEFT JOIN customers cust ON cust.id = NEW.customer_id AND cust.is_active = true
	 LEFT JOIN customerrateplan customerrateplan ON NEW.customer_rate_plan_id = customerrateplan.id
     LEFT JOIN customer_rate_pool customer_rate_pool ON NEW.customer_rate_pool_id = customer_rate_pool.id
	 LEFT JOIN vw_mobility_customer_current_cycle_device_usage ON vw_mobility_customer_current_cycle_device_usage.mobility_device_id = smi.id AND vw_mobility_customer_current_cycle_device_usage.tenant_id = NEW.tenant_id
 WHERE new.id=sim_management_inventory.mdt_id;
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE TRIGGER after_insert_optimization_device
    AFTER INSERT
    ON public.optimization_device
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_optimization_smi();


CREATE OR REPLACE TRIGGER after_update_optimization_device
    AFTER UPDATE
    ON public.optimization_device
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi();

CREATE OR REPLACE TRIGGER after_insert_optimization_device_result
    AFTER INSERT
    ON public.optimization_device_result
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_optimization_smi_result_from_device_result();

CREATE OR REPLACE TRIGGER after_update_optimization_device_result
    AFTER UPDATE
    ON public.optimization_device_result
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi_result_from_device_result();

CREATE OR REPLACE TRIGGER after_insert_optimization_device_result_customer_chargeq
    AFTER INSERT
    ON public.optimization_device_result_customer_charge_queue
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_to_optimization_smi_result_customer_chargeq_frm_device();

CREATE OR REPLACE TRIGGER after_update_optimization_device_result_rate_plan_queue_device
    AFTER UPDATE
    ON public.optimization_device_result_rate_plan_queue
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi_result_rate_plan_queue_from_device();

CREATE OR REPLACE TRIGGER after_insert_optimization_mobility_device
    AFTER INSERT
    ON public.optimization_mobility_device
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_optimization_smi_from_mobility();

CREATE OR REPLACE TRIGGER after_update_optimization_mobility_device
    AFTER UPDATE
    ON public.optimization_mobility_device
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi_from_mobility();

CREATE OR REPLACE TRIGGER after_insert_optimization_mobility_device_result
    AFTER INSERT
    ON public.optimization_mobility_device_result
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_optimization_smi_result_from_mobility_device_result();

CREATE OR REPLACE TRIGGER after_update_optimization_mobility_device_result
    AFTER UPDATE
    ON public.optimization_mobility_device_result
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi_result_from_mobility_device_result();


CREATE OR REPLACE TRIGGER after_insert_optimization_mobility_device_result_cust_chargeq
    AFTER INSERT
    ON public.optimization_mobility_device_result_customer_charge_queue
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_to_optimization_smi_result_cust_chargeq_frm_mobility();

CREATE OR REPLACE TRIGGER after_update_opt_mobility_device_result_customer_chargeq
    AFTER UPDATE
    ON public.optimization_mobility_device_result_customer_charge_queue
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi_result_cust_chargeq_frm_mobility();

CREATE OR REPLACE TRIGGER after_optimization_mobility_device_result_rate_plan_queue
    AFTER INSERT
    ON public.optimization_mobility_device_result_rate_plan_queue
    FOR EACH ROW
    EXECUTE FUNCTION public.insertto_optimization_smi_result_rate_plan_queue_from_mobility();

CREATE OR REPLACE TRIGGER after_update_optimization_mobility_device_result_rate_planq
    AFTER UPDATE
    ON public.optimization_mobility_device_result_rate_plan_queue
    FOR EACH ROW
    EXECUTE FUNCTION public.update_optimization_smi_result_rate_plan_queue_frm_mobility();

CREATE OR REPLACE TRIGGER after_insert_device_history
    AFTER INSERT
    ON public.device_history
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_inventory_history();

CREATE OR REPLACE TRIGGER after_update_device_history
    AFTER UPDATE
    ON public.device_history
    FOR EACH ROW
    EXECUTE FUNCTION public.update_inventory_history();

CREATE OR REPLACE FUNCTION public.insert_to_smi_bulkchange_frm_m2m()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO public.sim_management_bulk_change_request (
        bulk_change_id,
        iccid,
        msisdn,
        ip_address,
        change_request,
        device_id,
        is_processed,
        has_errors,
        status,
        status_details,
        processed_date,
        processed_by,
        created_by,
        created_date,
        modified_by,
        modified_date,
        deleted_by,
        deleted_date,
        is_active,
        device_change_request_id,
        m2m_id
    )
    VALUES (
        NEW.bulk_change_id,
        NEW.iccid,
        NEW.msisdn,
        NEW.ip_address,
        NEW.change_request,
        NEW.device_id,
        NEW.is_processed,
        NEW.has_errors,
        NEW.status,
        NEW.status_details,
        NEW.processed_date,
        NEW.processed_by,
        NEW.created_by,
        NEW.created_date,
        NEW.modified_by,
        NEW.modified_date,
        NEW.deleted_by,
        NEW.deleted_date,
        NEW.is_active,
        NEW.device_change_request_id,
        NEW.id
    );

    RETURN NEW;
END;
$BODY$;
CREATE OR REPLACE TRIGGER insert_sim_management_bulk_chng_req
    AFTER INSERT
    ON public.m2m_device_change
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_to_smi_bulkchange_frm_m2m();

CREATE OR REPLACE FUNCTION public.update_to_smi_bulkchange_frm_m2m()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    UPDATE public.sim_management_bulk_change_request
    SET
        bulk_change_id = NEW.bulk_change_id,
        iccid = NEW.iccid,
        msisdn = NEW.msisdn,
        ip_address = NEW.ip_address,
        change_request = NEW.change_request,
        device_id = NEW.device_id,
        is_processed = NEW.is_processed,
        has_errors = NEW.has_errors,
        status = NEW.status,
        status_details = NEW.status_details,
        processed_date = NEW.processed_date,
        processed_by = NEW.processed_by,
        modified_by = NEW.modified_by,
        modified_date = NEW.modified_date,
        deleted_by = NEW.deleted_by,
        deleted_date = NEW.deleted_date,
        is_active = NEW.is_active,
        device_change_request_id = NEW.device_change_request_id
    WHERE m2m_id = new.id;

    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE TRIGGER update_sim_management_req
    AFTER UPDATE
    ON public.m2m_device_change
    FOR EACH ROW
    EXECUTE FUNCTION public.update_to_smi_bulkchange_frm_m2m();

CREATE OR REPLACE TRIGGER sim_management_bulk_change_request_trigger
    AFTER INSERT
    ON public.mobility_device_change
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_intosim_managementbulkchangerequestfrommobilitychange();

CREATE OR REPLACE TRIGGER sim_management_bulk_change_request_update_trigger
    AFTER UPDATE
    ON public.mobility_device_change
    FOR EACH ROW
    EXECUTE FUNCTION public.update_sim_management_bulk_change_request_from_mobility();
CREATE OR REPLACE TRIGGER after_insert_mobility_device_history
    AFTER INSERT
    ON public.mobility_device_history
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_inventory_history_from_mobility();


CREATE OR REPLACE TRIGGER after_update_mobility_device_history
    AFTER UPDATE
    ON public.mobility_device_history
    FOR EACH ROW
    EXECUTE FUNCTION public.update_inventory_history_from_mobility();

CREATE OR REPLACE TRIGGER insert_into_smi_from_device
    AFTER INSERT
    ON public.device
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_smi_test_from_device();

CREATE OR REPLACE TRIGGER update_into_smi_from_device
    AFTER UPDATE
    ON public.device
    FOR EACH ROW
    EXECUTE FUNCTION public.update_smi_test_from_device();


CREATE OR REPLACE TRIGGER update_smi_frm_device_tenant_trigger
    AFTER UPDATE
    ON public.device_tenant
    FOR EACH ROW
    EXECUTE FUNCTION public.update_smi_frm_device_tenant();

CREATE OR REPLACE TRIGGER after_update_mobility_device_test
    AFTER UPDATE
    ON public.mobility_device
    FOR EACH ROW
    EXECUTE FUNCTION public.update_into_sim_management_inventory_test_from_mobility();


CREATE OR REPLACE TRIGGER insert_into_smi_from_mobility_device
    AFTER INSERT
    ON public.mobility_device
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_into_sim_management_inventory_test_from_mobility();

CREATE OR REPLACE TRIGGER update_smi_frm_mobility_device_tenant_trigger
    AFTER UPDATE
    ON public.mobility_device_tenant
    FOR EACH ROW
    EXECUTE FUNCTION public.update_smi_frm_mobility_device_tenant();


CREATE OR REPLACE FUNCTION public.set_assigned_value()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Check if the service_provider_id already exists in the table
    IF EXISTS (SELECT 1 FROM public.carrier_rate_plan WHERE service_provider = NEW.service_provider) THEN
        -- If it exists, set assigned to false
        NEW.assigned := true;
    ELSE
        -- If it does not exist, set assigned to true
        NEW.assigned := false;
    END IF;

    -- Return the new record with the updated assigned value
    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.update_processed_date()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    NEW.processed_date = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE TRIGGER update_processed_date_trigger
    BEFORE UPDATE
    ON public.sim_management_bulk_change
    FOR EACH ROW
    EXECUTE FUNCTION public.update_processed_date();

CREATE OR REPLACE TRIGGER update_processed_date_triggerreq
    BEFORE UPDATE
    ON public.sim_management_bulk_change_request
    FOR EACH ROW
    EXECUTE FUNCTION public.update_processed_date();

CREATE OR REPLACE TRIGGER before_insert_carrier_rate_plan_setassign_value
    BEFORE INSERT
    ON public.carrier_rate_plan
    FOR EACH ROW
    EXECUTE FUNCTION public.set_assigned_value();


