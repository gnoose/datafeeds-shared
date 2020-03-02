--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.21
-- Dumped by pg_dump version 11.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;

--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner:
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION pg_stat_statements IS 'track execution statistics of all SQL statements executed';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner:
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


--
-- Name: account_status_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.account_status_enum AS ENUM (
    'setup',
    'new customer',
    'active',
    'retired'
);


ALTER TYPE public.account_status_enum OWNER TO gridium;

--
-- Name: account_type_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.account_type_enum AS ENUM (
    'free',
    'demo',
    'trial',
    'paid'
);


ALTER TYPE public.account_type_enum OWNER TO gridium;

--
-- Name: audit_verdict_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.audit_verdict_enum AS ENUM (
    'failed',
    'warning',
    'passed',
    'error'
);


ALTER TYPE public.audit_verdict_enum OWNER TO gridium;

--
-- Name: decomp_parts; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.decomp_parts AS ENUM (
    'base',
    'op',
    'temp',
    'total'
);


ALTER TYPE public.decomp_parts OWNER TO gridium;

--
-- Name: facts; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.facts AS ENUM (
    'base',
    'op',
    'temp',
    'total'
);


ALTER TYPE public.facts OWNER TO gridium;

--
-- Name: flow_direction_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.flow_direction_enum AS ENUM (
    'forward',
    'reverse'
);


ALTER TYPE public.flow_direction_enum OWNER TO gridium;

--
-- Name: generation_provider_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.generation_provider_enum AS ENUM (
    'utility-bundled',
    'tnd-only',
    'cca'
);


CREATE TYPE public.provider_type_enum AS ENUM (
    'utility-bundled',
    'tnd-only'
);


ALTER TYPE public.generation_provider_enum OWNER TO gridium;

--
-- Name: provider_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.provider_enum AS ENUM (
    'gridium',
    'ce'
);


ALTER TYPE public.provider_enum OWNER TO gridium;

--
-- Name: snapmeter_provisioning_workflow_state; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.snapmeter_provisioning_workflow_state AS ENUM (
    'failed',
    'begin',
    'authorized',
    'verified',
    'data_staged',
    'objects_created',
    'data_imported',
    'analytics_ran',
    'complete',
    'refresh_requested',
    'data_sources_revised',
    'deprovisioning_requested',
    'account_deactivated',
    'deprovisioning_complete'
);


ALTER TYPE public.snapmeter_provisioning_workflow_state OWNER TO gridium;

--
-- Name: workflow_state_enum; Type: TYPE; Schema: public; Owner: gridium
--

CREATE TYPE public.workflow_state_enum AS ENUM (
    'pending',
    'quarantined',
    'review',
    'done'
);


ALTER TYPE public.workflow_state_enum OWNER TO gridium;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: access_token; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.access_token (
    oid bigint NOT NULL,
    resource character varying[] NOT NULL,
    token character varying NOT NULL,
    groups character varying[],
    expires timestamp without time zone NOT NULL,
    uses integer
);


ALTER TABLE public.access_token OWNER TO gridium;

--
-- Name: access_token_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.access_token_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.access_token_oid_seq OWNER TO gridium;

--
-- Name: access_token_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.access_token_oid_seq OWNED BY public.access_token.oid;


--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.alembic_version OWNER TO gridium;

--
-- Name: analytic_identifier; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.analytic_identifier (
    oid bigint NOT NULL,
    analytic character varying(128),
    source bigint,
    stored character varying(128)
);


ALTER TABLE public.analytic_identifier OWNER TO gridium;

--
-- Name: analytic_run; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.analytic_run (
    oid bigint NOT NULL,
    analytics character varying(128),
    meter bigint,
    occurred timestamp without time zone,
    problem json,
    status character varying(128),
    uuid character varying(128),
    identifier bigint
);


ALTER TABLE public.analytic_run OWNER TO gridium;

--
-- Name: archive_fragment; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.archive_fragment (
    oid bigint NOT NULL,
    archive bigint,
    complete boolean,
    id character varying(256),
    identity character varying(128),
    "offset" integer,
    uuid character varying(128)
);


ALTER TABLE public.archive_fragment OWNER TO gridium;

--
-- Name: auth_alembic_version; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.auth_alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.auth_alembic_version OWNER TO gridium;

--
-- Name: auth_session; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.auth_session (
    id integer NOT NULL,
    user_id integer,
    created timestamp without time zone
);


ALTER TABLE public.auth_session OWNER TO gridium;

--
-- Name: auth_session_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.auth_session_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.auth_session_id_seq OWNER TO gridium;

--
-- Name: auth_session_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.auth_session_id_seq OWNED BY public.auth_session.id;


--
-- Name: auth_user; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.auth_user (
    id integer NOT NULL,
    email character varying,
    password character varying
);


ALTER TABLE public.auth_user OWNER TO gridium;

--
-- Name: auth_user_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.auth_user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.auth_user_id_seq OWNER TO gridium;

--
-- Name: auth_user_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.auth_user_id_seq OWNED BY public.auth_user.id;


--
-- Name: average_load_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.average_load_analytics (
    oid bigint NOT NULL,
    cluster json,
    day json,
    meter bigint,
    open json
);


ALTER TABLE public.average_load_analytics OWNER TO gridium;

--
-- Name: balance_point_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.balance_point_analytics (
    oid bigint NOT NULL,
    balance double precision,
    day character varying(128),
    detail json,
    meter bigint
);


ALTER TABLE public.balance_point_analytics OWNER TO gridium;

--
-- Name: balance_point_detail; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.balance_point_detail (
    oid bigint NOT NULL,
    dry_bulb_temp_f character varying(128),
    "group" character varying(128),
    k_w double precision,
    mode_peak_time boolean,
    model_response double precision,
    normalized_demand double precision,
    summary bigint,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.balance_point_detail OWNER TO gridium;

--
-- Name: balance_point_summary; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.balance_point_summary (
    oid bigint NOT NULL,
    balance_point double precision,
    meter bigint,
    period_type character varying(128)
);


ALTER TABLE public.balance_point_summary OWNER TO gridium;

--
-- Name: bill; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.bill (
    oid bigint NOT NULL,
    attachments json,
    closing date,
    cost double precision,
    initial date,
    items json,
    manual boolean,
    modified timestamp without time zone,
    peak double precision,
    service bigint,
    used double precision,
    notes character varying,
    visible boolean DEFAULT true NOT NULL
);


ALTER TABLE public.bill OWNER TO gridium;

--
-- Name: bill_audit; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.bill_audit (
    oid bigint NOT NULL,
    workflow_state public.workflow_state_enum NOT NULL,
    audit_verdict public.audit_verdict_enum,
    audit_issues json,
    audit_errors json,
    bill bigint NOT NULL,
    latest_audit timestamp without time zone,
    modified timestamp without time zone
);


ALTER TABLE public.bill_audit OWNER TO gridium;

--
-- Name: bill_audit_event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.bill_audit_event (
    oid bigint NOT NULL,
    occurred timestamp without time zone NOT NULL,
    source character varying NOT NULL,
    description character varying,
    meta json,
    audit bigint NOT NULL
);


ALTER TABLE public.bill_audit_event OWNER TO gridium;

--
-- Name: bill_audit_event_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.bill_audit_event_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.bill_audit_event_oid_seq OWNER TO gridium;

--
-- Name: bill_audit_event_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.bill_audit_event_oid_seq OWNED BY public.bill_audit_event.oid;


--
-- Name: bill_audit_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.bill_audit_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.bill_audit_oid_seq OWNER TO gridium;

--
-- Name: bill_audit_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.bill_audit_oid_seq OWNED BY public.bill_audit.oid;


--
-- Name: bill_old; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.bill_old (
    oid bigint NOT NULL,
    attachments json,
    closing date,
    cost double precision,
    initial date,
    items json,
    manual boolean,
    modified timestamp without time zone,
    peak double precision,
    service bigint,
    used double precision,
    audit_accepted boolean DEFAULT false NOT NULL,
    audit_complete boolean DEFAULT false,
    audit_notes json,
    audit_successful boolean DEFAULT false,
    audit_suppressed boolean,
    audit_timestamp timestamp without time zone,
    notes character varying
);


ALTER TABLE public.bill_old OWNER TO gridium;

--
-- Name: bill_summary; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.bill_summary (
    oid bigint,
    meter bigint,
    summary json
);


ALTER TABLE public.bill_summary OWNER TO gridium;

--
-- Name: bill_v2_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.bill_v2_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.bill_v2_oid_seq OWNER TO gridium;

--
-- Name: bill_v2_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.bill_v2_oid_seq OWNED BY public.bill.oid;


--
-- Name: budget_aggregation; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.budget_aggregation (
    oid bigint NOT NULL,
    actual_cost character varying(128),
    adjusted_days double precision,
    calculated_cost character varying(128),
    calculated_demand_cost character varying(128),
    calculated_use_cost character varying(128),
    close_day_kwh_percent double precision,
    closed_day_kwh double precision,
    closed_days integer,
    cycle bigint,
    era character varying(128),
    meter bigint,
    open_day_kwh double precision,
    open_days integer,
    operations_kwh double precision,
    rate_model_base_rate double precision,
    temp_kwh double precision,
    temp_response_kwh double precision,
    temp_weather_kwh double precision,
    total_days integer,
    total_kwh double precision,
    total_data_points integer,
    imputed_data_points integer
);


ALTER TABLE public.budget_aggregation OWNER TO gridium;

--
-- Name: building; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.building (
    oid bigint NOT NULL,
    cardinal bigint,
    details json,
    forecast bigint,
    secondary bigint,
    source character varying(128),
    backup character varying(128),
    coordinates public.geometry(Point,4326),
    square_footage bigint,
    timezone character varying(128),
    street1 character varying(128),
    street2 character varying(128),
    city character varying(128),
    state character varying(2),
    zip character varying(10)
);


ALTER TABLE public.building OWNER TO gridium;

--
-- Name: building_calendar; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.building_calendar (
    oid bigint NOT NULL,
    building bigint,
    days json,
    holidays json,
    year integer
);


ALTER TABLE public.building_calendar OWNER TO gridium;

--
-- Name: building_occupancy; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.building_occupancy (
    oid bigint NOT NULL,
    building bigint,
    month date,
    occupancy numeric,
    actual boolean
);


ALTER TABLE public.building_occupancy OWNER TO gridium;

--
-- Name: building_occupancy_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.building_occupancy_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.building_occupancy_oid_seq OWNER TO gridium;

--
-- Name: building_occupancy_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.building_occupancy_oid_seq OWNED BY public.building_occupancy.oid;


--
-- Name: c3p0; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.c3p0 (
    a character(1)
);


ALTER TABLE public.c3p0 OWNER TO gridium;

--
-- Name: ce_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.ce_account (
    oid bigint NOT NULL,
    account character varying(256),
    contact character varying(256),
    origin bigint,
    parent character varying(256),
    smd_enrolled boolean,
    smd_subscription character varying(128),
    linked_count integer,
    snapmeter_account bigint
);


ALTER TABLE public.ce_account OWNER TO gridium;

--
-- Name: ce_orphan_meter_building; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.ce_orphan_meter_building (
    meter bigint,
    building bigint
);


ALTER TABLE public.ce_orphan_meter_building OWNER TO gridium;

--
-- Name: cluster_data; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.cluster_data (
    oid bigint NOT NULL,
    meter bigint,
    occurred date
);


ALTER TABLE public.cluster_data OWNER TO gridium;

--
-- Name: compare_day; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.compare_day (
    oid bigint NOT NULL,
    actual json,
    chart boolean,
    compare json,
    event date,
    meter bigint
);


ALTER TABLE public.compare_day OWNER TO gridium;

--
-- Name: computed_billing_cycle; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.computed_billing_cycle (
    oid bigint NOT NULL,
    closing date,
    initial date,
    meter bigint
);


ALTER TABLE public.computed_billing_cycle OWNER TO gridium;

--
-- Name: configuration; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.configuration (
    application character varying NOT NULL,
    configuration json
);


ALTER TABLE public.configuration OWNER TO gridium;

--
-- Name: configuration_backup; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.configuration_backup (
    application character varying,
    configuration json
);


ALTER TABLE public.configuration_backup OWNER TO gridium;

--
-- Name: curtailment_peak; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.curtailment_peak (
    oid bigint NOT NULL,
    curtailable boolean,
    occurred date,
    peak double precision,
    recommendation bigint
);


ALTER TABLE public.curtailment_peak OWNER TO gridium;

--
-- Name: curtailment_recommendation; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.curtailment_recommendation (
    oid bigint NOT NULL,
    cycle bigint,
    forecast_from date,
    forecast_to date,
    meter bigint,
    target double precision
);


ALTER TABLE public.curtailment_recommendation OWNER TO gridium;

--
-- Name: daily_budget_forecast; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.daily_budget_forecast (
    oid bigint NOT NULL,
    aggregate_data json,
    base_intervals double precision[],
    cdd integer,
    created timestamp without time zone,
    date date,
    dry_bulb_intervals double precision[],
    hdd integer,
    kw_intervals double precision[],
    meter bigint,
    open_day boolean,
    residual_intervals double precision[],
    scheduled_intervals double precision[],
    temp_intervals double precision[],
    type character varying(128)
);


ALTER TABLE public.daily_budget_forecast OWNER TO gridium;

--
-- Name: daily_fact; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.daily_fact (
    oid bigint NOT NULL,
    cluster integer,
    date date,
    elevated_base_use double precision,
    excess_overnight_use double precision,
    load_factor double precision,
    meter bigint,
    off_duration double precision,
    open_day boolean,
    overnight_base double precision,
    peak double precision,
    peak98 double precision,
    running_use double precision,
    start_duration double precision,
    start_hour double precision,
    start_time character varying(128),
    start_use double precision,
    stop_duration double precision,
    stop_hour double precision,
    stop_time character varying(128),
    stop_use double precision,
    use double precision,
    use_operational double precision,
    weekly_base double precision,
    use_baseload double precision,
    use_weather double precision,
    percent_imputed double precision
);


ALTER TABLE public.daily_fact OWNER TO gridium;

--
-- Name: daily_trend; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.daily_trend (
    oid bigint NOT NULL,
    date date,
    k_w02 double precision,
    k_w98 double precision,
    k_wh double precision,
    meter bigint
);


ALTER TABLE public.daily_trend OWNER TO gridium;

--
-- Name: database_archive; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.database_archive (
    oid bigint NOT NULL,
    completed timestamp without time zone,
    created timestamp without time zone,
    date date,
    endpoint character varying(128),
    vault character varying(128)
);


ALTER TABLE public.database_archive OWNER TO gridium;

--
-- Name: day_cluster_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.day_cluster_analytics (
    oid bigint NOT NULL,
    day_clusters json,
    meter bigint
);


ALTER TABLE public.day_cluster_analytics OWNER TO gridium;

--
-- Name: decomp_facts; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.decomp_facts (
    oid bigint NOT NULL,
    meter bigint NOT NULL,
    occurred timestamp without time zone,
    fact public.facts,
    value double precision
);


ALTER TABLE public.decomp_facts OWNER TO gridium;

--
-- Name: decomp_facts_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.decomp_facts_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.decomp_facts_oid_seq OWNER TO gridium;

--
-- Name: decomp_facts_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.decomp_facts_oid_seq OWNED BY public.decomp_facts.oid;


--
-- Name: decomposition_data; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.decomposition_data (
    oid bigint NOT NULL,
    data json,
    meter bigint,
    occurred date
);


ALTER TABLE public.decomposition_data OWNER TO gridium;

--
-- Name: degree_day; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.degree_day (
    oid bigint NOT NULL,
    cdd integer,
    date date,
    hdd integer,
    source bigint
);


ALTER TABLE public.degree_day OWNER TO gridium;

--
-- Name: drift_report; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.drift_report (
    oid bigint NOT NULL,
    days json,
    meter bigint,
    updated timestamp without time zone
);


ALTER TABLE public.drift_report OWNER TO gridium;

--
-- Name: employee; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.employee (
    oid bigint NOT NULL,
    account character varying(128),
    email character varying(128),
    name character varying(128),
    roles json,
    status character varying(128)
);


ALTER TABLE public.employee OWNER TO gridium;

--
-- Name: entry; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.entry (
    oid bigint NOT NULL,
    content character varying(128),
    kind character varying(128),
    plan json,
    queue character varying(128),
    scheduled bigint
);


ALTER TABLE public.entry OWNER TO gridium;

--
-- Name: error_model; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.error_model (
    oid bigint NOT NULL,
    cv double precision,
    kind character varying(128),
    mape double precision,
    r2 double precision,
    rmse double precision,
    run bigint,
    version integer
);


ALTER TABLE public.error_model OWNER TO gridium;

--
-- Name: event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.event (
    oid bigint NOT NULL,
    begin character varying(128),
    date date,
    "end" character varying(128),
    program character varying(128),
    type character varying(128)
);


ALTER TABLE public.event OWNER TO gridium;

--
-- Name: event_status; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.event_status (
    oid bigint NOT NULL,
    event date,
    meter bigint,
    pattern json,
    results character varying(128),
    use json
);


ALTER TABLE public.event_status OWNER TO gridium;

--
-- Name: fit_dr_model_data; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.fit_dr_model_data (
    oid bigint NOT NULL,
    meter bigint,
    occurred date
);


ALTER TABLE public.fit_dr_model_data OWNER TO gridium;

--
-- Name: fit_dr_model_summary; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.fit_dr_model_summary (
    oid bigint NOT NULL,
    meter bigint,
    occurred date,
    summary json
);


ALTER TABLE public.fit_dr_model_summary OWNER TO gridium;

--
-- Name: forecast_dr_evaluation; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.forecast_dr_evaluation (
    oid bigint NOT NULL,
    compares json,
    curve json,
    meter bigint,
    occurred date,
    patterns json,
    range json,
    use json,
    variety character varying(128)
);


ALTER TABLE public.forecast_dr_evaluation OWNER TO gridium;

--
-- Name: forecast_location; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.forecast_location (
    oid bigint NOT NULL,
    coordinates public.geometry(Point,4326)
);


ALTER TABLE public.forecast_location OWNER TO gridium;

--
-- Name: forecast_model_stats; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.forecast_model_stats (
    oid bigint NOT NULL,
    meter bigint,
    cv double precision
);


ALTER TABLE public.forecast_model_stats OWNER TO gridium;

--
-- Name: foreign_system_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.foreign_system_account (
    oid bigint NOT NULL,
    login json,
    password json,
    system character varying(128)
);


ALTER TABLE public.foreign_system_account OWNER TO gridium;

--
-- Name: foreign_system_attribute; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.foreign_system_attribute (
    oid bigint NOT NULL,
    reference bigint,
    system bigint,
    "values" json
);


ALTER TABLE public.foreign_system_attribute OWNER TO gridium;

--
-- Name: green_button_customer; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_customer (
    oid bigint NOT NULL,
    identifier character varying(128),
    name character varying(128),
    retail bigint,
    self character varying(256)
);


ALTER TABLE public.green_button_customer OWNER TO gridium;

--
-- Name: green_button_customer_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_customer_account (
    oid bigint NOT NULL,
    customer bigint,
    identifier character varying(128),
    name character varying(128),
    self character varying(256)
);


ALTER TABLE public.green_button_customer_account OWNER TO gridium;

--
-- Name: green_button_customer_agreement; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_customer_agreement (
    oid bigint NOT NULL,
    account bigint,
    address json,
    identifier character varying(128),
    name character varying(128),
    tariff character varying(128),
    self character varying(256)
);


ALTER TABLE public.green_button_customer_agreement OWNER TO gridium;

--
-- Name: green_button_gap_fill_job; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_gap_fill_job (
    oid bigint NOT NULL,
    point bigint NOT NULL,
    start date NOT NULL,
    "end" date NOT NULL,
    tries integer NOT NULL,
    latest_task character varying,
    error character varying,
    datatype character varying
);


ALTER TABLE public.green_button_gap_fill_job OWNER TO gridium;

--
-- Name: green_button_gap_fill_job_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_gap_fill_job_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_gap_fill_job_oid_seq OWNER TO gridium;

--
-- Name: green_button_gap_fill_job_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_gap_fill_job_oid_seq OWNED BY public.green_button_gap_fill_job.oid;


--
-- Name: green_button_interval_block; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_interval_block (
    oid bigint NOT NULL,
    identifier character varying(128),
    interval_duration integer,
    qualities json,
    reading bigint,
    readings json,
    start bigint,
    total_duration integer
);


ALTER TABLE public.green_button_interval_block OWNER TO gridium;

--
-- Name: green_button_meter_reading; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_meter_reading (
    oid bigint NOT NULL,
    identifier character varying(128),
    point bigint,
    reading_type bigint
);


ALTER TABLE public.green_button_meter_reading OWNER TO gridium;

--
-- Name: green_button_notification; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_notification (
    oid integer NOT NULL,
    provider_oid integer NOT NULL,
    xml character varying NOT NULL,
    notification_time timestamp without time zone NOT NULL
);


ALTER TABLE public.green_button_notification OWNER TO gridium;

--
-- Name: green_button_notification_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_notification_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_notification_oid_seq OWNER TO gridium;

--
-- Name: green_button_notification_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_notification_oid_seq OWNED BY public.green_button_notification.oid;


--
-- Name: green_button_notification_resource; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_notification_resource (
    oid integer NOT NULL,
    notification_oid integer NOT NULL,
    resource_url character varying NOT NULL
);


ALTER TABLE public.green_button_notification_resource OWNER TO gridium;

--
-- Name: green_button_notification_resource_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_notification_resource_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_notification_resource_oid_seq OWNER TO gridium;

--
-- Name: green_button_notification_resource_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_notification_resource_oid_seq OWNED BY public.green_button_notification_resource.oid;


--
-- Name: green_button_notification_task; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_notification_task (
    oid integer NOT NULL,
    task_oid integer NOT NULL,
    owner_oid integer NOT NULL
);


ALTER TABLE public.green_button_notification_task OWNER TO gridium;

--
-- Name: green_button_notification_task_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_notification_task_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_notification_task_oid_seq OWNER TO gridium;

--
-- Name: green_button_notification_task_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_notification_task_oid_seq OWNED BY public.green_button_notification_task.oid;


--
-- Name: green_button_provider; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_provider (
    oid integer NOT NULL,
    utility character varying NOT NULL,
    identifier character varying NOT NULL
);


ALTER TABLE public.green_button_provider OWNER TO gridium;

--
-- Name: green_button_provider_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_provider_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_provider_oid_seq OWNER TO gridium;

--
-- Name: green_button_provider_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_provider_oid_seq OWNED BY public.green_button_provider.oid;


--
-- Name: green_button_reading_stats; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_reading_stats (
    oid bigint NOT NULL,
    reading bigint NOT NULL,
    start timestamp without time zone,
    "end" timestamp without time zone,
    intervals_missing integer,
    intervals_missing_tail integer,
    intervals_total integer,
    intervals_zero integer,
    missing_first timestamp without time zone,
    missing_last timestamp without time zone,
    gap_count integer,
    gap_length_max integer,
    gap_length_avg integer,
    gaps json,
    last_analysis timestamp without time zone NOT NULL,
    error character varying
);


ALTER TABLE public.green_button_reading_stats OWNER TO gridium;

--
-- Name: green_button_reading_stats_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_reading_stats_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_reading_stats_oid_seq OWNER TO gridium;

--
-- Name: green_button_reading_stats_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_reading_stats_oid_seq OWNED BY public.green_button_reading_stats.oid;


--
-- Name: green_button_reading_type; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_reading_type (
    oid bigint NOT NULL,
    accumulation_behaviour integer,
    commodity integer,
    currency integer,
    data_qualifier integer,
    default_quality integer,
    flow_direction integer,
    identifier character varying(128),
    interval_length integer,
    kind integer,
    measuring_period integer,
    phase integer,
    power_of_ten_multiplier integer,
    self character varying(256),
    time_attribute integer,
    uom integer
);


ALTER TABLE public.green_button_reading_type OWNER TO gridium;

--
-- Name: green_button_retail_customer; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_retail_customer (
    oid bigint NOT NULL,
    identifier character varying(128),
    provider bigint,
    self character varying(256)
);


ALTER TABLE public.green_button_retail_customer OWNER TO gridium;

--
-- Name: green_button_subscription_task; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_subscription_task (
    oid integer NOT NULL,
    task_oid integer NOT NULL,
    subscription character varying NOT NULL,
    url character varying NOT NULL
);


ALTER TABLE public.green_button_subscription_task OWNER TO gridium;

--
-- Name: green_button_subscription_task_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_subscription_task_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_subscription_task_oid_seq OWNER TO gridium;

--
-- Name: green_button_subscription_task_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_subscription_task_oid_seq OWNED BY public.green_button_subscription_task.oid;


--
-- Name: green_button_task; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_task (
    oid integer NOT NULL,
    celery_task character varying NOT NULL,
    state character varying NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp without time zone NOT NULL,
    error character varying,
    key character varying,
    provider_oid integer NOT NULL
);


ALTER TABLE public.green_button_task OWNER TO gridium;

--
-- Name: green_button_task_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.green_button_task_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.green_button_task_oid_seq OWNER TO gridium;

--
-- Name: green_button_task_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.green_button_task_oid_seq OWNED BY public.green_button_task.oid;


--
-- Name: green_button_time_parameters; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_time_parameters (
    oid bigint NOT NULL,
    dst_end_rule character varying(128),
    dst_offset integer,
    dst_start_rule character varying(128),
    identifier character varying(128),
    self character varying(256),
    tz_offset integer
);


ALTER TABLE public.green_button_time_parameters OWNER TO gridium;

--
-- Name: green_button_usage_point; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_usage_point (
    oid bigint NOT NULL,
    identifier character varying(128),
    kind character varying(128),
    retail bigint,
    self character varying(256),
    time_parameters bigint
);


ALTER TABLE public.green_button_usage_point OWNER TO gridium;

--
-- Name: green_button_usage_summary; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.green_button_usage_summary (
    oid bigint NOT NULL,
    bill_last_period bigint,
    commodity integer,
    consumption json,
    currency integer,
    details json,
    duration integer,
    identifier character varying(128),
    point bigint,
    read_cycle character varying(128),
    self character varying(256),
    start bigint,
    status_time_stamp bigint,
    tariff_profile character varying(128)
);


ALTER TABLE public.green_button_usage_summary OWNER TO gridium;

--
-- Name: hours_at_demand; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.hours_at_demand (
    oid bigint NOT NULL,
    count integer,
    demand double precision,
    meter bigint
);


ALTER TABLE public.hours_at_demand OWNER TO gridium;

--
-- Name: integration_test_run; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.integration_test_run (
    oid bigint NOT NULL,
    occurred date,
    results json
);


ALTER TABLE public.integration_test_run OWNER TO gridium;

--
-- Name: interval_facts; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.interval_facts (
    oid bigint NOT NULL,
    changepoint_base double precision,
    changepoint_mean double precision,
    changepoint_prob double precision,
    cycling_flag boolean,
    daily bigint,
    decomp_base double precision,
    decomp_residual double precision,
    decomp_scheduled double precision,
    decomp_temp double precision,
    deviation_prob double precision,
    hardstart_flag boolean,
    imputed boolean,
    kw double precision,
    load_state character varying(128),
    needlepeak_flag boolean,
    running_time boolean,
    "time" character varying(128),
    decomp_temp_response double precision,
    decomp_temp_weather double precision
);


ALTER TABLE public.interval_facts OWNER TO gridium;

--
-- Name: kntest_sq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.kntest_sq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.kntest_sq OWNER TO gridium;

--
-- Name: kntest; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.kntest (
    oid bigint DEFAULT nextval('public.kntest_sq'::regclass) NOT NULL,
    value bigint
);


ALTER TABLE public.kntest OWNER TO gridium;

--
-- Name: latest_snapmeter; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.latest_snapmeter (
    oid bigint NOT NULL,
    created timestamp without time zone,
    meter bigint,
    report json
);


ALTER TABLE public.latest_snapmeter OWNER TO gridium;

--
-- Name: load_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.load_analytics (
    oid bigint NOT NULL,
    hours_at_demand json,
    load_duration json,
    meter bigint
);


ALTER TABLE public.load_analytics OWNER TO gridium;

--
-- Name: load_by_day; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.load_by_day (
    oid bigint NOT NULL,
    day_of_week character varying(128),
    mean_demand double precision,
    meter bigint,
    "time" character varying(128)
);


ALTER TABLE public.load_by_day OWNER TO gridium;

--
-- Name: load_duration; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.load_duration (
    oid bigint NOT NULL,
    k_w double precision,
    meter bigint,
    percentile double precision
);


ALTER TABLE public.load_duration OWNER TO gridium;

--
-- Name: message_template; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.message_template (
    oid bigint NOT NULL,
    mongo character varying,
    name character varying NOT NULL,
    body character varying NOT NULL,
    headline character varying NOT NULL,
    source character varying DEFAULT 'analyst'::character varying NOT NULL,
    category character varying NOT NULL,
    created timestamp without time zone,
    injection integer DEFAULT 0
);


ALTER TABLE public.message_template OWNER TO gridium;

--
-- Name: message_template_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.message_template_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.message_template_oid_seq OWNER TO gridium;

--
-- Name: message_template_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.message_template_oid_seq OWNED BY public.message_template.oid;


--
-- Name: messaging_alembic_version; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.messaging_alembic_version OWNER TO gridium;

--
-- Name: messaging_email; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_email (
    id integer NOT NULL,
    created timestamp without time zone,
    state character varying,
    from_addr character varying,
    to_addr character varying[],
    subject character varying,
    text text,
    html text,
    domain character varying,
    cc character varying[],
    bcc character varying[],
    replyto character varying,
    attachments character varying[],
    custom json,
    mailgun_identifier character varying,
    disable_click_tracking boolean
);


ALTER TABLE public.messaging_email OWNER TO gridium;

--
-- Name: messaging_email_event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_email_event (
    id integer NOT NULL,
    received timestamp without time zone,
    email_id integer,
    body json
);


ALTER TABLE public.messaging_email_event OWNER TO gridium;

--
-- Name: messaging_email_events_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.messaging_email_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.messaging_email_events_id_seq OWNER TO gridium;

--
-- Name: messaging_email_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.messaging_email_events_id_seq OWNED BY public.messaging_email_event.id;


--
-- Name: messaging_email_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.messaging_email_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.messaging_email_id_seq OWNER TO gridium;

--
-- Name: messaging_email_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.messaging_email_id_seq OWNED BY public.messaging_email.id;


--
-- Name: messaging_incoming_email; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_incoming_email (
    id integer NOT NULL,
    created timestamp without time zone,
    from_addr character varying,
    sender character varying,
    to_addr character varying,
    recipient character varying,
    subject character varying,
    raw_text text,
    raw_html text,
    stripped_text text,
    stripped_html text,
    reply_target_id integer,
    attachments json
);


ALTER TABLE public.messaging_incoming_email OWNER TO gridium;

--
-- Name: messaging_incoming_email_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.messaging_incoming_email_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.messaging_incoming_email_id_seq OWNER TO gridium;

--
-- Name: messaging_incoming_email_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.messaging_incoming_email_id_seq OWNED BY public.messaging_incoming_email.id;


--
-- Name: messaging_sms_incoming_message; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_sms_incoming_message (
    id integer NOT NULL,
    created timestamp without time zone,
    from_number character varying,
    to_number character varying,
    body character varying,
    twilio_identifier character varying
);


ALTER TABLE public.messaging_sms_incoming_message OWNER TO gridium;

--
-- Name: messaging_sms_incoming_message_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.messaging_sms_incoming_message_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.messaging_sms_incoming_message_id_seq OWNER TO gridium;

--
-- Name: messaging_sms_incoming_message_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.messaging_sms_incoming_message_id_seq OWNED BY public.messaging_sms_incoming_message.id;


--
-- Name: messaging_sms_message; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_sms_message (
    id integer NOT NULL,
    created timestamp without time zone,
    state character varying,
    from_number character varying,
    to_number character varying,
    body character varying,
    custom json,
    twilio_identifier character varying
);


ALTER TABLE public.messaging_sms_message OWNER TO gridium;

--
-- Name: messaging_sms_message_event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.messaging_sms_message_event (
    id integer NOT NULL,
    received timestamp without time zone,
    message_id integer,
    body json
);


ALTER TABLE public.messaging_sms_message_event OWNER TO gridium;

--
-- Name: messaging_sms_message_event_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.messaging_sms_message_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.messaging_sms_message_event_id_seq OWNER TO gridium;

--
-- Name: messaging_sms_message_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.messaging_sms_message_event_id_seq OWNED BY public.messaging_sms_message_event.id;


--
-- Name: messaging_sms_message_id_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.messaging_sms_message_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.messaging_sms_message_id_seq OWNER TO gridium;

--
-- Name: messaging_sms_message_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.messaging_sms_message_id_seq OWNED BY public.messaging_sms_message.id;


--
-- Name: meter; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter (
    oid bigint NOT NULL,
    billing character varying(128),
    building bigint,
    commodity character varying(128),
    "interval" integer,
    kind character varying(128),
    name character varying(128),
    number character varying(128),
    parent bigint,
    point character varying(128),
    service bigint,
    system bigint,
    direction public.flow_direction_enum DEFAULT 'forward'::public.flow_direction_enum,
    CONSTRAINT valid_meter_direction CHECK ((direction IS NOT NULL))
);


ALTER TABLE public.meter OWNER TO gridium;

--
-- Name: meter_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_analytics (
    oid bigint NOT NULL,
    days json,
    meter bigint,
    period integer
);


ALTER TABLE public.meter_analytics OWNER TO gridium;

--
-- Name: meter_bulk_edit_log; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_bulk_edit_log (
    oid bigint NOT NULL,
    created timestamp without time zone,
    editor bigint NOT NULL,
    meter bigint NOT NULL,
    updates jsonb
);


ALTER TABLE public.meter_bulk_edit_log OWNER TO gridium;

--
-- Name: meter_bulk_edit_log_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.meter_bulk_edit_log_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.meter_bulk_edit_log_oid_seq OWNER TO gridium;

--
-- Name: meter_bulk_edit_log_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.meter_bulk_edit_log_oid_seq OWNED BY public.meter_bulk_edit_log.oid;


--
-- Name: meter_group; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_group (
    oid bigint NOT NULL,
    account character varying NOT NULL,
    name character varying NOT NULL,
    shared boolean NOT NULL,
    "user" character varying NOT NULL,
    modified timestamp without time zone
);


ALTER TABLE public.meter_group OWNER TO gridium;

--
-- Name: meter_group_item; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_group_item (
    oid bigint NOT NULL,
    "group" bigint NOT NULL,
    meter bigint NOT NULL
);


ALTER TABLE public.meter_group_item OWNER TO gridium;

--
-- Name: meter_group_item_bak; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_group_item_bak (
    oid bigint,
    "group" bigint,
    meter bigint
);


ALTER TABLE public.meter_group_item_bak OWNER TO gridium;

--
-- Name: meter_group_item_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.meter_group_item_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.meter_group_item_oid_seq OWNER TO gridium;

--
-- Name: meter_group_item_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.meter_group_item_oid_seq OWNED BY public.meter_group_item.oid;


--
-- Name: meter_group_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.meter_group_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.meter_group_oid_seq OWNER TO gridium;

--
-- Name: meter_group_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.meter_group_oid_seq OWNED BY public.meter_group.oid;


--
-- Name: meter_message; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_message (
    oid bigint NOT NULL,
    mongo character varying,
    account_hex character varying NOT NULL,
    meter bigint NOT NULL,
    author_hex character varying,
    headline character varying NOT NULL,
    body character varying NOT NULL,
    value double precision,
    reference timestamp without time zone,
    message_type character varying NOT NULL,
    source character varying,
    created timestamp without time zone NOT NULL,
    report date NOT NULL,
    status character varying,
    template bigint,
    message_dates timestamp without time zone[]
);


ALTER TABLE public.meter_message OWNER TO gridium;

--
-- Name: meter_message_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.meter_message_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.meter_message_oid_seq OWNER TO gridium;

--
-- Name: meter_message_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.meter_message_oid_seq OWNED BY public.meter_message.oid;


--
-- Name: meter_reading; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.meter_reading (
    oid bigint NOT NULL,
    meter bigint,
    occurred date,
    readings json,
    frozen boolean
);


ALTER TABLE public.meter_reading OWNER TO gridium;

--
-- Name: meter_reading_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.meter_reading_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.meter_reading_seq OWNER TO gridium;

--
-- Name: meter_reading_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.meter_reading_seq OWNED BY public.meter_reading.oid;


--
-- Name: snapmeter_meter_data_source; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_meter_data_source (
    oid bigint NOT NULL,
    hex_id character varying,
    meter bigint,
    name character varying,
    account_data_source bigint,
    meta jsonb,
    source_types character varying[]
);


ALTER TABLE public.snapmeter_meter_data_source OWNER TO gridium;

--
-- Name: utility_service; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.utility_service (
    oid bigint NOT NULL,
    account bigint,
    active boolean DEFAULT true NOT NULL,
    "group" character varying(128),
    options json,
    service_id character varying(128),
    tariff character varying(128),
    utility character varying(128),
    utility_account_id character varying,
    provider_type public.provider_type_enum DEFAULT 'utility-bundled'::public.provider_type_enum NOT NULL,
    gen_service_id character varying(128),
    gen_tariff character varying(128),
    gen_utility character varying(128),
    gen_utility_account_id character varying,
    gen_options json
);


ALTER TABLE public.utility_service OWNER TO gridium;

--
-- Name: model_statistic; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.model_statistic (
    oid bigint NOT NULL,
    meter bigint,
    metric character varying(128),
    type character varying(128),
    value double precision
);


ALTER TABLE public.model_statistic OWNER TO gridium;

--
-- Name: monthly_budget_forecast; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.monthly_budget_forecast (
    oid bigint NOT NULL,
    created timestamp without time zone,
    "end" date,
    forecast_stats json,
    high_forecast_stats json,
    low_forecast_stats json,
    meter bigint,
    occupied_days integer,
    start date,
    updated timestamp without time zone
);


ALTER TABLE public.monthly_budget_forecast OWNER TO gridium;

--
-- Name: monthly_fact; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.monthly_fact (
    oid bigint NOT NULL,
    base_demand double precision,
    closed_use double precision,
    meter bigint,
    month_end date,
    n_days integer,
    open_use double precision,
    peak double precision
);


ALTER TABLE public.monthly_fact OWNER TO gridium;

--
-- Name: monthly_forecast; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.monthly_forecast (
    oid bigint NOT NULL,
    billing_month_from date,
    billing_month_id bigint,
    billing_month_to date,
    calculated_cost double precision,
    cdd double precision,
    hdd double precision,
    kwh double precision,
    meter bigint,
    num_closed integer,
    num_open integer,
    rate_model_base_rate double precision,
    type character varying(128),
    use_closed double precision,
    use_open double precision,
    use_temp double precision
);


ALTER TABLE public.monthly_forecast OWNER TO gridium;

--
-- Name: monthly_yoy_variance; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.monthly_yoy_variance (
    oid bigint NOT NULL,
    actual json,
    baseline json,
    created timestamp without time zone,
    "end" date,
    meter bigint,
    start date,
    updated timestamp without time zone
);


ALTER TABLE public.monthly_yoy_variance OWNER TO gridium;

--
-- Name: mv_assessment_timeseries; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_assessment_timeseries (
    oid bigint NOT NULL,
    program bigint NOT NULL,
    meter bigint,
    occurred date NOT NULL,
    "interval" integer NOT NULL,
    predicted json NOT NULL,
    baseload json NOT NULL
);


ALTER TABLE public.mv_assessment_timeseries OWNER TO gridium;

--
-- Name: mv_assessment_timeseries_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_assessment_timeseries_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_assessment_timeseries_oid_seq OWNER TO gridium;

--
-- Name: mv_assessment_timeseries_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_assessment_timeseries_oid_seq OWNED BY public.mv_assessment_timeseries.oid;


--
-- Name: mv_baseline; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_baseline (
    oid bigint NOT NULL,
    baseline double precision,
    baseline_fit date,
    decomp_base double precision,
    decomp_temp double precision,
    meter bigint,
    period timestamp without time zone,
    decomp_scheduled double precision
);


ALTER TABLE public.mv_baseline OWNER TO gridium;

--
-- Name: mv_baseline_timeseries; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_baseline_timeseries (
    oid bigint NOT NULL,
    program bigint NOT NULL,
    meter bigint,
    occurred date NOT NULL,
    "interval" integer NOT NULL,
    predicted json NOT NULL
);


ALTER TABLE public.mv_baseline_timeseries OWNER TO gridium;

--
-- Name: mv_baseline_timeseries_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_baseline_timeseries_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_baseline_timeseries_oid_seq OWNER TO gridium;

--
-- Name: mv_baseline_timeseries_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_baseline_timeseries_oid_seq OWNED BY public.mv_baseline_timeseries.oid;


--
-- Name: mv_drift_data; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_drift_data (
    oid bigint NOT NULL,
    baseline json,
    baseload_baseline_total double precision,
    baseload_comparison_total double precision,
    comparison json,
    operations_baseline_total double precision,
    operations_comparison_total double precision,
    period bigint,
    temperature_baseline_total double precision,
    temperature_comparison_total double precision
);


ALTER TABLE public.mv_drift_data OWNER TO gridium;

--
-- Name: mv_drift_period; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_drift_period (
    oid bigint NOT NULL,
    baseline_period date,
    comparison_end date,
    comparison_start date,
    meter bigint
);


ALTER TABLE public.mv_drift_period OWNER TO gridium;

--
-- Name: mv_exogenous_factor_timeseries; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_exogenous_factor_timeseries (
    oid bigint NOT NULL,
    program bigint NOT NULL,
    meter bigint,
    name character varying NOT NULL,
    occurred date NOT NULL,
    "interval" integer NOT NULL,
    "values" json NOT NULL
);


ALTER TABLE public.mv_exogenous_factor_timeseries OWNER TO gridium;

--
-- Name: mv_exogenous_factor_timeseries_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_exogenous_factor_timeseries_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_exogenous_factor_timeseries_oid_seq OWNER TO gridium;

--
-- Name: mv_exogenous_factor_timeseries_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_exogenous_factor_timeseries_oid_seq OWNED BY public.mv_exogenous_factor_timeseries.oid;


--
-- Name: mv_model_fit_statistic; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_model_fit_statistic (
    oid bigint NOT NULL,
    program bigint NOT NULL,
    meter bigint,
    kind character varying NOT NULL,
    metric character varying NOT NULL,
    value double precision NOT NULL
);


ALTER TABLE public.mv_model_fit_statistic OWNER TO gridium;

--
-- Name: mv_model_fit_statistic_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_model_fit_statistic_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_model_fit_statistic_oid_seq OWNER TO gridium;

--
-- Name: mv_model_fit_statistic_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_model_fit_statistic_oid_seq OWNED BY public.mv_model_fit_statistic.oid;


--
-- Name: mv_nonroutine_event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_nonroutine_event (
    oid bigint NOT NULL,
    program bigint NOT NULL,
    meter bigint,
    occurred date NOT NULL,
    kind character varying NOT NULL,
    pvalue double precision NOT NULL
);


ALTER TABLE public.mv_nonroutine_event OWNER TO gridium;

--
-- Name: mv_nonroutine_event_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_nonroutine_event_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_nonroutine_event_oid_seq OWNER TO gridium;

--
-- Name: mv_nonroutine_event_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_nonroutine_event_oid_seq OWNED BY public.mv_nonroutine_event.oid;


--
-- Name: mv_program; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_program (
    oid bigint NOT NULL,
    name character varying NOT NULL
);


ALTER TABLE public.mv_program OWNER TO gridium;

--
-- Name: mv_program_cross_type; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_program_cross_type (
    oid bigint NOT NULL,
    program_type bigint NOT NULL,
    program bigint NOT NULL
);


ALTER TABLE public.mv_program_cross_type OWNER TO gridium;

--
-- Name: mv_program_cross_type_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_program_cross_type_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_program_cross_type_oid_seq OWNER TO gridium;

--
-- Name: mv_program_cross_type_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_program_cross_type_oid_seq OWNED BY public.mv_program_cross_type.oid;


--
-- Name: mv_program_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_program_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_program_oid_seq OWNER TO gridium;

--
-- Name: mv_program_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_program_oid_seq OWNED BY public.mv_program.oid;


--
-- Name: mv_program_type; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_program_type (
    oid bigint NOT NULL,
    name character varying NOT NULL,
    identifier character varying NOT NULL
);


ALTER TABLE public.mv_program_type OWNER TO gridium;

--
-- Name: mv_program_type_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_program_type_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_program_type_oid_seq OWNER TO gridium;

--
-- Name: mv_program_type_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_program_type_oid_seq OWNED BY public.mv_program_type.oid;


--
-- Name: mv_project; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.mv_project (
    oid bigint NOT NULL,
    program bigint NOT NULL,
    customer bigint NOT NULL,
    building bigint,
    meter_group bigint,
    weather_station bigint,
    description character varying,
    baseline_start date NOT NULL,
    baseline_end date NOT NULL,
    assessment_start date
);


ALTER TABLE public.mv_project OWNER TO gridium;

--
-- Name: mv_project_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.mv_project_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mv_project_oid_seq OWNER TO gridium;

--
-- Name: mv_project_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.mv_project_oid_seq OWNED BY public.mv_project.oid;


--
-- Name: obvius_meter; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.obvius_meter (
    oid bigint NOT NULL,
    das_serial character varying NOT NULL,
    channel character varying NOT NULL,
    meter bigint NOT NULL
);


ALTER TABLE public.obvius_meter OWNER TO gridium;

--
-- Name: obvius_meter_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.obvius_meter_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.obvius_meter_oid_seq OWNER TO gridium;

--
-- Name: obvius_meter_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.obvius_meter_oid_seq OWNED BY public.obvius_meter.oid;


--
-- Name: pdp_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pdp_analytics (
    oid bigint NOT NULL,
    forecasts json,
    meter bigint,
    rates json,
    segmentation character varying(128),
    summary json
);


ALTER TABLE public.pdp_analytics OWNER TO gridium;

--
-- Name: pdp_event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pdp_event (
    event date NOT NULL,
    meter bigint NOT NULL,
    cardinal bigint NOT NULL,
    secondary bigint NOT NULL,
    has_interval boolean NOT NULL,
    has_weather boolean NOT NULL
);


ALTER TABLE public.pdp_event OWNER TO gridium;

--
-- Name: pdp_event_interval; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pdp_event_interval (
    event date NOT NULL,
    meter bigint NOT NULL,
    has_data boolean NOT NULL
);


ALTER TABLE public.pdp_event_interval OWNER TO gridium;

--
-- Name: pdp_event_weather; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pdp_event_weather (
    event date NOT NULL,
    station bigint NOT NULL,
    has_data boolean NOT NULL
);


ALTER TABLE public.pdp_event_weather OWNER TO gridium;

--
-- Name: peak_billing_fact; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.peak_billing_fact (
    oid bigint NOT NULL,
    billing character varying(128),
    cost double precision,
    cycle bigint,
    daily character varying(128),
    imputed boolean,
    kw double precision,
    meter bigint,
    predicted_cost double precision,
    predicted_kw double precision,
    timing character varying(128),
    demand_cost double precision,
    predicted_demand_cost double precision,
    peak_date date,
    peak_time character varying(128)
);


ALTER TABLE public.peak_billing_fact OWNER TO gridium;

--
-- Name: peak_forecast; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.peak_forecast (
    oid bigint NOT NULL,
    billing_end date,
    billing_start date,
    date date,
    high_temp_f character varying(128),
    holiday boolean,
    last_day boolean,
    low_temp_f character varying(128),
    lower_ci double precision,
    meter bigint,
    predicted_peak double precision,
    predicted_prob double precision,
    tou character varying(128),
    upper_ci double precision,
    utility_event boolean,
    holiday_name character varying(128)
);


ALTER TABLE public.peak_forecast OWNER TO gridium;

--
-- Name: peak_history; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.peak_history (
    oid bigint NOT NULL,
    actual_peak double precision,
    billing_end date,
    billing_start date,
    date date,
    high_temp_f character varying(128),
    holiday boolean,
    low_temp_f character varying(128),
    meter bigint,
    predicted_peak double precision,
    predicted_prob double precision,
    tou character varying(128),
    utility_event boolean,
    holiday_name character varying(128),
    incomplete boolean
);


ALTER TABLE public.peak_history OWNER TO gridium;

--
-- Name: peak_prediction; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.peak_prediction (
    oid bigint NOT NULL,
    meter bigint,
    occurred date,
    predictions json
);


ALTER TABLE public.peak_prediction OWNER TO gridium;

--
-- Name: pge_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pge_account (
    oid bigint NOT NULL,
    account_number character varying(128),
    ce bigint,
    date date,
    mailing_address character varying(128)
);


ALTER TABLE public.pge_account OWNER TO gridium;

--
-- Name: pge_bill; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pge_bill (
    oid bigint NOT NULL,
    account bigint,
    bill_date date,
    cost double precision,
    key character varying(128)
);


ALTER TABLE public.pge_bill OWNER TO gridium;

--
-- Name: pge_credential; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.pge_credential (
    oid bigint NOT NULL,
    password_bytes bytea,
    username_bytes bytea
);


ALTER TABLE public.pge_credential OWNER TO gridium;

--
-- Name: pge_credential_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.pge_credential_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.pge_credential_oid_seq OWNER TO gridium;

--
-- Name: pge_credential_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.pge_credential_oid_seq OWNED BY public.pge_credential.oid;


--
-- Name: plotting_fact; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.plotting_fact (
    oid bigint NOT NULL,
    fact character varying(128),
    meter bigint,
    occurred timestamp without time zone,
    value double precision
);


ALTER TABLE public.plotting_fact OWNER TO gridium;

--
-- Name: product_enrollment; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.product_enrollment (
    oid bigint NOT NULL,
    meter bigint,
    product character varying(128),
    status character varying(128)
);


ALTER TABLE public.product_enrollment OWNER TO gridium;

--
-- Name: provision_entry; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.provision_entry (
    oid bigint NOT NULL,
    name character varying(128),
    pass json,
    status character varying(128),
    "user" character varying(128)
);


ALTER TABLE public.provision_entry OWNER TO gridium;

--
-- Name: provision_extract; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.provision_extract (
    oid bigint NOT NULL,
    account character varying(128),
    address json,
    created timestamp without time zone,
    entry bigint,
    meter character varying(128),
    pdp boolean,
    service_id character varying(128),
    tariff json
);


ALTER TABLE public.provision_extract OWNER TO gridium;

--
-- Name: provision_origin; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.provision_origin (
    oid bigint NOT NULL,
    pass character varying(128),
    supplements json,
    "user" character varying(128)
);


ALTER TABLE public.provision_origin OWNER TO gridium;

--
-- Name: rate_analysis; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.rate_analysis (
    oid bigint NOT NULL,
    analysis json,
    current character varying(128),
    meter bigint,
    utility character varying(128),
    "interval" json,
    billing json
);


ALTER TABLE public.rate_analysis OWNER TO gridium;

--
-- Name: rate_model_coefficient; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.rate_model_coefficient (
    oid bigint NOT NULL,
    costmeasure character varying(128),
    created timestamp without time zone,
    intercept double precision,
    mape double precision,
    max_demand double precision,
    max_demand_interaction double precision,
    meter bigint,
    rsquare double precision,
    third_party_adjustment double precision,
    total_use double precision,
    total_use_interaction double precision,
    updated timestamp without time zone
);


ALTER TABLE public.rate_model_coefficient OWNER TO gridium;

--
-- Name: rate_right; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.rate_right (
    oid bigint NOT NULL,
    billing json,
    bills integer,
    charges json,
    current json,
    gaps json,
    "interval" json,
    meter bigint,
    recommended json,
    said character varying(128),
    savings character varying(128),
    garbage json
);


ALTER TABLE public.rate_right OWNER TO gridium;

--
-- Name: rate_right_summary; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.rate_right_summary (
    oid bigint NOT NULL,
    meter bigint,
    summary json
);


ALTER TABLE public.rate_right_summary OWNER TO gridium;

--
-- Name: rcx_pattern_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.rcx_pattern_analytics (
    oid bigint NOT NULL,
    days json,
    intervals json,
    meter bigint,
    period integer
);


ALTER TABLE public.rcx_pattern_analytics OWNER TO gridium;

--
-- Name: real_time_attribute_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.real_time_attribute_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.real_time_attribute_seq OWNER TO gridium;

--
-- Name: real_time_attribute; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.real_time_attribute (
    oid bigint DEFAULT nextval('public.real_time_attribute_seq'::regclass) NOT NULL,
    meter bigint,
    multiplier double precision,
    notes character varying(128),
    das_serial character varying,
    channel character varying,
    token bigint,
    device character varying DEFAULT 'Internal I/O'::character varying NOT NULL
);


ALTER TABLE public.real_time_attribute OWNER TO gridium;

--
-- Name: recommendation_indices; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.recommendation_indices (
    oid bigint NOT NULL,
    indices json,
    meter bigint
);


ALTER TABLE public.recommendation_indices OWNER TO gridium;

--
-- Name: report; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.report (
    oid bigint NOT NULL,
    account character varying NOT NULL,
    "user" character varying NOT NULL,
    title character varying NOT NULL,
    level character varying NOT NULL,
    "interval" character varying NOT NULL,
    range_type character varying NOT NULL,
    variables jsonb DEFAULT '[]'::jsonb,
    subtotal character varying,
    range_span character varying,
    range_unit character varying,
    range_start timestamp without time zone,
    range_end timestamp without time zone,
    contextual character varying[] DEFAULT '{}'::character varying[],
    "group" integer
);


ALTER TABLE public.report OWNER TO gridium;

--
-- Name: report_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.report_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.report_oid_seq OWNER TO gridium;

--
-- Name: report_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.report_oid_seq OWNED BY public.report.oid;


--
-- Name: rtm_monitor; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.rtm_monitor (
    meter bigint NOT NULL,
    active boolean,
    last_state_change timestamp without time zone,
    last_state_check timestamp without time zone,
    comment character varying
);


ALTER TABLE public.rtm_monitor OWNER TO gridium;

--
-- Name: rtm_monitor_meter_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.rtm_monitor_meter_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.rtm_monitor_meter_seq OWNER TO gridium;

--
-- Name: rtm_monitor_meter_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.rtm_monitor_meter_seq OWNED BY public.rtm_monitor.meter;


--
-- Name: savings_estimates; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.savings_estimates (
    oid bigint NOT NULL,
    avg_closed_excess_savings double precision,
    avg_excess_baseload_savings double precision,
    avg_lowered_baseload_savings double precision,
    avg_start_duration_savings double precision,
    avg_start_time_savings double precision,
    avg_stop_duration_savings double precision,
    avg_stop_time_savings double precision,
    daily_savings json,
    meter bigint,
    targets json,
    updated timestamp without time zone
);


ALTER TABLE public.savings_estimates OWNER TO gridium;

--
-- Name: sce_gb_customer_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.sce_gb_customer_account (
    oid bigint NOT NULL,
    identifier character varying NOT NULL,
    name character varying,
    retail_customer_oid bigint NOT NULL
);


ALTER TABLE public.sce_gb_customer_account OWNER TO gridium;

--
-- Name: sce_gb_customer_account_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.sce_gb_customer_account_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sce_gb_customer_account_oid_seq OWNER TO gridium;

--
-- Name: sce_gb_customer_account_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.sce_gb_customer_account_oid_seq OWNED BY public.sce_gb_customer_account.oid;


--
-- Name: sce_gb_customer_agreement; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.sce_gb_customer_agreement (
    oid bigint NOT NULL,
    said character varying NOT NULL,
    usage_point character varying NOT NULL,
    tariff character varying NOT NULL,
    address jsonb NOT NULL,
    account_oid bigint NOT NULL
);


ALTER TABLE public.sce_gb_customer_agreement OWNER TO gridium;

--
-- Name: sce_gb_customer_agreement_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.sce_gb_customer_agreement_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sce_gb_customer_agreement_oid_seq OWNER TO gridium;

--
-- Name: sce_gb_customer_agreement_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.sce_gb_customer_agreement_oid_seq OWNED BY public.sce_gb_customer_agreement.oid;


--
-- Name: sce_gb_resource; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.sce_gb_resource (
    oid bigint NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp without time zone NOT NULL,
    resources character varying NOT NULL,
    collected boolean NOT NULL,
    task_id character varying
);


ALTER TABLE public.sce_gb_resource OWNER TO gridium;

--
-- Name: sce_gb_resource_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.sce_gb_resource_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sce_gb_resource_oid_seq OWNER TO gridium;

--
-- Name: sce_gb_resource_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.sce_gb_resource_oid_seq OWNED BY public.sce_gb_resource.oid;


--
-- Name: sce_gb_retail_customer; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.sce_gb_retail_customer (
    oid bigint NOT NULL,
    identifier character varying NOT NULL,
    name character varying
);


ALTER TABLE public.sce_gb_retail_customer OWNER TO gridium;

--
-- Name: sce_gb_retail_customer_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.sce_gb_retail_customer_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sce_gb_retail_customer_oid_seq OWNER TO gridium;

--
-- Name: sce_gb_retail_customer_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.sce_gb_retail_customer_oid_seq OWNED BY public.sce_gb_retail_customer.oid;


--
-- Name: score_pattern_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.score_pattern_analytics (
    oid bigint NOT NULL,
    days json,
    intervals json,
    meter bigint,
    period integer
);


ALTER TABLE public.score_pattern_analytics OWNER TO gridium;

--
-- Name: scrooge_ops_audit; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.scrooge_ops_audit (
    oid bigint NOT NULL,
    meter bigint,
    summary json
);


ALTER TABLE public.scrooge_ops_audit OWNER TO gridium;

--
-- Name: scrooge_ops_audit_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.scrooge_ops_audit_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.scrooge_ops_audit_oid_seq OWNER TO gridium;

--
-- Name: scrooge_ops_audit_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.scrooge_ops_audit_oid_seq OWNED BY public.scrooge_ops_audit.oid;


--
-- Name: service_summary; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.service_summary (
    oid bigint NOT NULL,
    meter bigint,
    summary json
);


ALTER TABLE public.service_summary OWNER TO gridium;

--
-- Name: smd_authorization_audit; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.smd_authorization_audit (
    oid bigint NOT NULL,
    occurred timestamp without time zone NOT NULL,
    completed boolean NOT NULL,
    credential bigint NOT NULL,
    authorized boolean NOT NULL,
    validated_credentials boolean NOT NULL
);


ALTER TABLE public.smd_authorization_audit OWNER TO gridium;

--
-- Name: smd_authorization_audit_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.smd_authorization_audit_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.smd_authorization_audit_oid_seq OWNER TO gridium;

--
-- Name: smd_authorization_audit_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.smd_authorization_audit_oid_seq OWNED BY public.smd_authorization_audit.oid;


--
-- Name: smd_authorization_audit_point; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.smd_authorization_audit_point (
    oid bigint NOT NULL,
    audit bigint NOT NULL,
    retail_customer_id character varying NOT NULL,
    service_id character varying NOT NULL
);


ALTER TABLE public.smd_authorization_audit_point OWNER TO gridium;

--
-- Name: smd_authorization_audit_point_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.smd_authorization_audit_point_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.smd_authorization_audit_point_oid_seq OWNER TO gridium;

--
-- Name: smd_authorization_audit_point_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.smd_authorization_audit_point_oid_seq OWNED BY public.smd_authorization_audit_point.oid;


--
-- Name: smd_subscription_detail; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.smd_subscription_detail (
    oid bigint NOT NULL,
    provider public.provider_enum NOT NULL,
    subscription character varying NOT NULL,
    access_token character varying NOT NULL,
    refresh_token character varying NOT NULL,
    resource_uri character varying NOT NULL,
    authorization_uri character varying NOT NULL,
    updated timestamp without time zone NOT NULL
);


ALTER TABLE public.smd_subscription_detail OWNER TO gridium;

--
-- Name: smd_subscription_detail_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.smd_subscription_detail_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.smd_subscription_detail_oid_seq OWNER TO gridium;

--
-- Name: smd_subscription_detail_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.smd_subscription_detail_oid_seq OWNED BY public.smd_subscription_detail.oid;


--
-- Name: snapmeter_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_account (
    oid bigint NOT NULL,
    hex_id character varying,
    account_type public.account_type_enum NOT NULL,
    created timestamp without time zone NOT NULL,
    domain character varying NOT NULL,
    name character varying NOT NULL,
    status public.account_status_enum NOT NULL,
    token_login boolean NOT NULL
);


ALTER TABLE public.snapmeter_account OWNER TO gridium;

--
-- Name: snapmeter_account_data_source; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_account_data_source (
    oid bigint NOT NULL,
    hex_id character varying,
    account bigint NOT NULL,
    source_account_type character varying NOT NULL,
    name character varying NOT NULL,
    username_bytes bytea,
    password_bytes bytea,
    enabled boolean DEFAULT true NOT NULL
);


ALTER TABLE public.snapmeter_account_data_source OWNER TO gridium;

--
-- Name: snapmeter_account_data_source_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_account_data_source_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_account_data_source_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_account_data_source_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_account_data_source_oid_seq OWNED BY public.snapmeter_account_data_source.oid;


--
-- Name: snapmeter_account_meter; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_account_meter (
    account bigint NOT NULL,
    meter bigint NOT NULL,
    utility_account_id character varying,
    estimated_changes jsonb,
    created timestamp without time zone,
    oid bigint NOT NULL,
    generation_provider public.generation_provider_enum DEFAULT 'utility-bundled'::public.generation_provider_enum NOT NULL,
    snapmeter_delivery boolean DEFAULT true NOT NULL
);


ALTER TABLE public.snapmeter_account_meter OWNER TO gridium;

--
-- Name: snapmeter_account_meter_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_account_meter_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_account_meter_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_account_meter_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_account_meter_oid_seq OWNED BY public.snapmeter_account_meter.oid;


--
-- Name: snapmeter_account_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_account_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_account_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_account_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_account_oid_seq OWNED BY public.snapmeter_account.oid;


--
-- Name: snapmeter_account_user; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_account_user (
    oid bigint NOT NULL,
    account bigint NOT NULL,
    "user" bigint NOT NULL
);


ALTER TABLE public.snapmeter_account_user OWNER TO gridium;

--
-- Name: snapmeter_account_user_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_account_user_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_account_user_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_account_user_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_account_user_oid_seq OWNED BY public.snapmeter_account_user.oid;


--
-- Name: snapmeter_announcement; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_announcement (
    oid bigint NOT NULL,
    headline character varying NOT NULL,
    body character varying NOT NULL,
    expires date NOT NULL
);


ALTER TABLE public.snapmeter_announcement OWNER TO gridium;

--
-- Name: snapmeter_announcement_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_announcement_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_announcement_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_announcement_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_announcement_oid_seq OWNED BY public.snapmeter_announcement.oid;


--
-- Name: snapmeter_bill_view; Type: MATERIALIZED VIEW; Schema: public; Owner: gridium
--

CREATE MATERIALIZED VIEW public.snapmeter_bill_view AS
 SELECT bill_old.oid,
    bill_old.closing,
    bill_old.cost,
    bill_old.initial,
    bill_old.peak,
    bill_old.service,
    bill_old.used,
    bill_old.audit_accepted,
    bill_old.audit_complete,
    bill_old.audit_successful,
    bill_old.audit_suppressed
   FROM public.bill_old
  WHERE (bill_old.service IN ( SELECT m.service
           FROM public.meter m,
            public.snapmeter_account_meter sam
          WHERE (sam.meter = m.oid)))
  WITH NO DATA;


ALTER TABLE public.snapmeter_bill_view OWNER TO gridium;

--
-- Name: snapmeter_building; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_building (
    building bigint NOT NULL,
    account bigint NOT NULL,
    name character varying NOT NULL,
    energy_star integer,
    visible boolean DEFAULT false NOT NULL
);


ALTER TABLE public.snapmeter_building OWNER TO gridium;

--
-- Name: snapmeter_data_gap; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_data_gap (
    oid bigint NOT NULL,
    account bigint NOT NULL,
    meter bigint NOT NULL,
    said character varying,
    source character varying,
    gap_type character varying NOT NULL,
    from_dt timestamp without time zone NOT NULL,
    to_dt timestamp without time zone NOT NULL,
    as_of timestamp without time zone
);


ALTER TABLE public.snapmeter_data_gap OWNER TO gridium;

--
-- Name: snapmeter_data_gap_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_data_gap_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_data_gap_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_data_gap_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_data_gap_oid_seq OWNED BY public.snapmeter_data_gap.oid;


--
-- Name: snapmeter_data_quality_snapshot; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_data_quality_snapshot (
    oid bigint NOT NULL,
    account bigint NOT NULL,
    meter bigint NOT NULL,
    interval_source character varying NOT NULL,
    billing_source character varying NOT NULL,
    interval_time_stale interval NOT NULL,
    interval_time_missing interval NOT NULL,
    bill_time_stale interval NOT NULL,
    bill_time_missing interval NOT NULL,
    analytics_time_stale interval NOT NULL,
    as_of timestamp without time zone NOT NULL,
    weather_time_historical interval,
    weather_time_stale interval,
    weather_forecast_source bigint,
    weather_history_source bigint,
    interval_data_exists boolean,
    bill_data_exists boolean,
    analytics_data_exists boolean
);


ALTER TABLE public.snapmeter_data_quality_snapshot OWNER TO gridium;

--
-- Name: snapmeter_data_quality_snapshot_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_data_quality_snapshot_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_data_quality_snapshot_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_data_quality_snapshot_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_data_quality_snapshot_oid_seq OWNED BY public.snapmeter_data_quality_snapshot.oid;


--
-- Name: snapmeter_failed_login; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_failed_login (
    oid bigint NOT NULL,
    email character varying NOT NULL,
    login_dt timestamp without time zone NOT NULL,
    lockout_until_dt timestamp without time zone
);


ALTER TABLE public.snapmeter_failed_login OWNER TO gridium;

--
-- Name: snapmeter_failed_login_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_failed_login_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_failed_login_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_failed_login_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_failed_login_oid_seq OWNED BY public.snapmeter_failed_login.oid;


--
-- Name: snapmeter_image; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_image (
    oid bigint NOT NULL,
    run_date timestamp without time zone NOT NULL,
    meter_id bigint NOT NULL
);


ALTER TABLE public.snapmeter_image OWNER TO gridium;

--
-- Name: snapmeter_image_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_image_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_image_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_image_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_image_oid_seq OWNED BY public.snapmeter_image.oid;


--
-- Name: snapmeter_meter_data_source_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_meter_data_source_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_meter_data_source_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_meter_data_source_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_meter_data_source_oid_seq OWNED BY public.snapmeter_meter_data_source.oid;


--
-- Name: snapmeter_provisioning; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_provisioning (
    oid bigint NOT NULL,
    active boolean NOT NULL,
    parent_account_name character varying,
    account bigint,
    salesforce_contract_id character varying
);


ALTER TABLE public.snapmeter_provisioning OWNER TO gridium;

--
-- Name: snapmeter_provisioning_credential; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_provisioning_credential (
    oid bigint NOT NULL,
    password_bytes bytea NOT NULL,
    username_bytes bytea NOT NULL
);


ALTER TABLE public.snapmeter_provisioning_credential OWNER TO gridium;

--
-- Name: snapmeter_provisioning_credential_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_provisioning_credential_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_provisioning_credential_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_provisioning_credential_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_provisioning_credential_oid_seq OWNED BY public.snapmeter_provisioning_credential.oid;


--
-- Name: snapmeter_provisioning_event; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_provisioning_event (
    oid bigint NOT NULL,
    workflow bigint NOT NULL,
    occurred timestamp without time zone NOT NULL,
    error boolean NOT NULL,
    state public.snapmeter_provisioning_workflow_state NOT NULL,
    message character varying NOT NULL,
    meta json
);


ALTER TABLE public.snapmeter_provisioning_event OWNER TO gridium;

--
-- Name: snapmeter_provisioning_event_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_provisioning_event_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_provisioning_event_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_provisioning_event_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_provisioning_event_oid_seq OWNED BY public.snapmeter_provisioning_event.oid;


--
-- Name: snapmeter_provisioning_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_provisioning_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_provisioning_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_provisioning_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_provisioning_oid_seq OWNED BY public.snapmeter_provisioning.oid;


--
-- Name: snapmeter_provisioning_workflow; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_provisioning_workflow (
    oid bigint NOT NULL,
    parent bigint NOT NULL,
    credential bigint NOT NULL,
    complete boolean
);


ALTER TABLE public.snapmeter_provisioning_workflow OWNER TO gridium;

--
-- Name: snapmeter_provisioning_workflow_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_provisioning_workflow_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_provisioning_workflow_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_provisioning_workflow_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_provisioning_workflow_oid_seq OWNED BY public.snapmeter_provisioning_workflow.oid;


--
-- Name: snapmeter_user; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_user (
    oid bigint NOT NULL,
    hex_id character varying,
    email character varying NOT NULL,
    password character varying,
    name character varying,
    groups character varying[],
    meta jsonb
);


ALTER TABLE public.snapmeter_user OWNER TO gridium;

--
-- Name: snapmeter_user_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_user_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_user_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_user_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_user_oid_seq OWNED BY public.snapmeter_user.oid;


--
-- Name: snapmeter_user_subscription; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.snapmeter_user_subscription (
    oid bigint NOT NULL,
    "user" bigint NOT NULL,
    subscription character varying,
    meter bigint,
    sent timestamp without time zone
);


ALTER TABLE public.snapmeter_user_subscription OWNER TO gridium;

--
-- Name: snapmeter_user_subscription_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.snapmeter_user_subscription_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapmeter_user_subscription_oid_seq OWNER TO gridium;

--
-- Name: snapmeter_user_subscription_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.snapmeter_user_subscription_oid_seq OWNED BY public.snapmeter_user_subscription.oid;


--
-- Name: standard_holiday; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.standard_holiday (
    oid bigint NOT NULL,
    day character varying(128),
    "from" date,
    occupancy double precision,
    "to" date,
    utilization double precision,
    year integer
);


ALTER TABLE public.standard_holiday OWNER TO gridium;

--
-- Name: stasis; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.stasis (
    oid bigint NOT NULL,
    objects json
);


ALTER TABLE public.stasis OWNER TO gridium;

--
-- Name: stasis_transaction; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.stasis_transaction (
    oid bigint NOT NULL,
    created timestamp without time zone,
    problem json,
    stasis bigint,
    status character varying(128),
    target bigint
);


ALTER TABLE public.stasis_transaction OWNER TO gridium;

--
-- Name: temp_building; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.temp_building (
    oid bigint,
    cardinal bigint,
    details json,
    forecast bigint,
    secondary bigint,
    source character varying(128),
    backup character varying(128)
);


ALTER TABLE public.temp_building OWNER TO gridium;

--
-- Name: temp_forecast_location; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.temp_forecast_location (
    oid bigint,
    location json
);


ALTER TABLE public.temp_forecast_location OWNER TO gridium;

--
-- Name: temp_weather_station; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.temp_weather_station (
    oid bigint,
    code character varying(128),
    coordinates json,
    descriptor character varying(128),
    name character varying(128),
    wban integer
);


ALTER TABLE public.temp_weather_station OWNER TO gridium;

--
-- Name: temperature_response; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.temperature_response (
    oid bigint NOT NULL,
    day_of_week character varying(128),
    dry_bulb_temp_f character varying(128),
    meter bigint,
    mode_peak_time boolean,
    model_response double precision,
    period_type character varying(128),
    "time" character varying(128)
);


ALTER TABLE public.temperature_response OWNER TO gridium;

--
-- Name: test_stored; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.test_stored (
    oid bigint NOT NULL,
    coordinates public.geometry(Point,4326),
    descriptions json,
    "end" date,
    mix_case_thing character varying(128),
    name character varying(128),
    num integer,
    parent bigint
);


ALTER TABLE public.test_stored OWNER TO gridium;

--
-- Name: trailing_twelve_month_analytics; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.trailing_twelve_month_analytics (
    oid bigint NOT NULL,
    meter bigint,
    ttms json
);


ALTER TABLE public.trailing_twelve_month_analytics OWNER TO gridium;

--
-- Name: ttm_calculation; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.ttm_calculation (
    oid bigint NOT NULL,
    excess_baseload_consistancy double precision,
    excess_baseload_overall double precision,
    excess_closed_use double precision,
    excess_start_duration double precision,
    excess_start_time double precision,
    excess_stop_duration double precision,
    excess_stop_time double precision,
    open_days integer,
    period bigint
);


ALTER TABLE public.ttm_calculation OWNER TO gridium;

--
-- Name: ttm_fact; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.ttm_fact (
    oid bigint NOT NULL,
    period bigint,
    value double precision,
    variable character varying(128)
);


ALTER TABLE public.ttm_fact OWNER TO gridium;

--
-- Name: ttm_period; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.ttm_period (
    oid bigint NOT NULL,
    meter bigint,
    n_days integer,
    year_end date,
    year_start date
);


ALTER TABLE public.ttm_period OWNER TO gridium;

--
-- Name: usage_history; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.usage_history (
    oid bigint NOT NULL,
    actual_use double precision,
    billing_end date,
    billing_start date,
    high_temp_f character varying(128),
    holiday boolean,
    low_temp_f character varying(128),
    meter bigint,
    occurred date,
    predicted_use double precision,
    utility_event boolean,
    incomplete boolean
);


ALTER TABLE public.usage_history OWNER TO gridium;

--
-- Name: use_prediction; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.use_prediction (
    oid bigint NOT NULL,
    meter bigint,
    occurred date,
    predictions json
);


ALTER TABLE public.use_prediction OWNER TO gridium;

--
-- Name: utility; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.utility (
    oid bigint NOT NULL,
    identifier character varying NOT NULL,
    name character varying NOT NULL
);


ALTER TABLE public.utility OWNER TO gridium;

--
-- Name: utility_account; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.utility_account (
    oid bigint NOT NULL,
    account_id character varying(128),
    system bigint,
    utility character varying(128)
);


ALTER TABLE public.utility_account OWNER TO gridium;

--
-- Name: utility_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.utility_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.utility_oid_seq OWNER TO gridium;

--
-- Name: utility_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.utility_oid_seq OWNED BY public.utility.oid;


--
-- Name: utility_tariff; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.utility_tariff (
    oid bigint NOT NULL,
    tariff character varying NOT NULL,
    description character varying NOT NULL,
    utility character varying NOT NULL
);


ALTER TABLE public.utility_tariff OWNER TO gridium;

--
-- Name: utility_tariff_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.utility_tariff_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.utility_tariff_oid_seq OWNER TO gridium;

--
-- Name: utility_tariff_oid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gridium
--

ALTER SEQUENCE public.utility_tariff_oid_seq OWNED BY public.utility_tariff.oid;


--
-- Name: variance_clause; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.variance_clause (
    oid bigint NOT NULL,
    analysis bigint,
    baseline bigint,
    calendar double precision,
    calendar_adj double precision,
    cost_calendar double precision,
    cost_demand double precision,
    cost_operations double precision,
    cost_rate double precision,
    cost_temp_response double precision,
    cost_temp_weather double precision,
    cost_total double precision,
    cost_tou double precision,
    operations double precision,
    operations_adj double precision,
    percent_calendar double precision,
    percent_demand double precision,
    percent_operations double precision,
    percent_rate double precision,
    percent_temp_response double precision,
    percent_temp_weather double precision,
    percent_total double precision,
    percent_tou double precision,
    rate_cost_adj double precision,
    temp_response double precision,
    temp_response_adj double precision,
    temp_weather double precision,
    temp_weather_adj double precision,
    total double precision,
    total_adj double precision,
    tou_cost_adj double precision,
    percent_imputed double precision
);


ALTER TABLE public.variance_clause OWNER TO gridium;

--
-- Name: weather_forecast; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.weather_forecast (
    oid bigint NOT NULL,
    location bigint,
    metrics json,
    occurrence date
);


ALTER TABLE public.weather_forecast OWNER TO gridium;

--
-- Name: weather_history; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.weather_history (
    oid bigint NOT NULL,
    metrics json,
    occurrence date,
    source bigint
);


ALTER TABLE public.weather_history OWNER TO gridium;

--
-- Name: weather_history_log; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.weather_history_log (
    oid bigint NOT NULL,
    date date,
    history bigint,
    metrics json,
    occurred timestamp without time zone,
    source bigint,
    wds character varying(128)
);


ALTER TABLE public.weather_history_log OWNER TO gridium;

--
-- Name: weather_source_axis; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.weather_source_axis (
    oid bigint NOT NULL,
    city character varying(128),
    coordinates public.geometry(Point,4326),
    us character varying(128),
    zip character varying(128)
);


ALTER TABLE public.weather_source_axis OWNER TO gridium;

--
-- Name: weather_station; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.weather_station (
    oid bigint NOT NULL,
    code character varying(128),
    descriptor character varying(128),
    name character varying(128),
    wban integer,
    coordinates public.geometry(Point,4326)
);


ALTER TABLE public.weather_station OWNER TO gridium;

--
-- Name: webapps_platform_oid_adapter_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.webapps_platform_oid_adapter_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.webapps_platform_oid_adapter_seq OWNER TO gridium;

--
-- Name: workflow_task_run; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.workflow_task_run (
    oid bigint NOT NULL,
    completed timestamp without time zone,
    problem json,
    started timestamp without time zone,
    task character varying(128)
);


ALTER TABLE public.workflow_task_run OWNER TO gridium;

--
-- Name: wsi_axis_oid_seq; Type: SEQUENCE; Schema: public; Owner: gridium
--

CREATE SEQUENCE public.wsi_axis_oid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.wsi_axis_oid_seq OWNER TO gridium;

--
-- Name: wsi_axis; Type: TABLE; Schema: public; Owner: gridium
--

CREATE TABLE public.wsi_axis (
    oid bigint DEFAULT nextval('public.wsi_axis_oid_seq'::regclass) NOT NULL,
    coordinates public.geometry(Point,4326),
    region character varying(128)
);


ALTER TABLE public.wsi_axis OWNER TO gridium;

--
-- Name: access_token oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.access_token ALTER COLUMN oid SET DEFAULT nextval('public.access_token_oid_seq'::regclass);


--
-- Name: auth_session id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.auth_session ALTER COLUMN id SET DEFAULT nextval('public.auth_session_id_seq'::regclass);


--
-- Name: auth_user id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.auth_user ALTER COLUMN id SET DEFAULT nextval('public.auth_user_id_seq'::regclass);


--
-- Name: bill oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill ALTER COLUMN oid SET DEFAULT nextval('public.bill_v2_oid_seq'::regclass);


--
-- Name: bill_audit oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill_audit ALTER COLUMN oid SET DEFAULT nextval('public.bill_audit_oid_seq'::regclass);


--
-- Name: bill_audit_event oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill_audit_event ALTER COLUMN oid SET DEFAULT nextval('public.bill_audit_event_oid_seq'::regclass);


--
-- Name: building_occupancy oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.building_occupancy ALTER COLUMN oid SET DEFAULT nextval('public.building_occupancy_oid_seq'::regclass);


--
-- Name: decomp_facts oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.decomp_facts ALTER COLUMN oid SET DEFAULT nextval('public.decomp_facts_oid_seq'::regclass);


--
-- Name: green_button_gap_fill_job oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_gap_fill_job ALTER COLUMN oid SET DEFAULT nextval('public.green_button_gap_fill_job_oid_seq'::regclass);


--
-- Name: green_button_notification oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification ALTER COLUMN oid SET DEFAULT nextval('public.green_button_notification_oid_seq'::regclass);


--
-- Name: green_button_notification_resource oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_resource ALTER COLUMN oid SET DEFAULT nextval('public.green_button_notification_resource_oid_seq'::regclass);


--
-- Name: green_button_notification_task oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_task ALTER COLUMN oid SET DEFAULT nextval('public.green_button_notification_task_oid_seq'::regclass);


--
-- Name: green_button_provider oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_provider ALTER COLUMN oid SET DEFAULT nextval('public.green_button_provider_oid_seq'::regclass);


--
-- Name: green_button_reading_stats oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_reading_stats ALTER COLUMN oid SET DEFAULT nextval('public.green_button_reading_stats_oid_seq'::regclass);


--
-- Name: green_button_subscription_task oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_subscription_task ALTER COLUMN oid SET DEFAULT nextval('public.green_button_subscription_task_oid_seq'::regclass);


--
-- Name: green_button_task oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_task ALTER COLUMN oid SET DEFAULT nextval('public.green_button_task_oid_seq'::regclass);


--
-- Name: message_template oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.message_template ALTER COLUMN oid SET DEFAULT nextval('public.message_template_oid_seq'::regclass);


--
-- Name: messaging_email id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_email ALTER COLUMN id SET DEFAULT nextval('public.messaging_email_id_seq'::regclass);


--
-- Name: messaging_email_event id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_email_event ALTER COLUMN id SET DEFAULT nextval('public.messaging_email_events_id_seq'::regclass);


--
-- Name: messaging_incoming_email id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_incoming_email ALTER COLUMN id SET DEFAULT nextval('public.messaging_incoming_email_id_seq'::regclass);


--
-- Name: messaging_sms_incoming_message id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_incoming_message ALTER COLUMN id SET DEFAULT nextval('public.messaging_sms_incoming_message_id_seq'::regclass);


--
-- Name: messaging_sms_message id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_message ALTER COLUMN id SET DEFAULT nextval('public.messaging_sms_message_id_seq'::regclass);


--
-- Name: messaging_sms_message_event id; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_message_event ALTER COLUMN id SET DEFAULT nextval('public.messaging_sms_message_event_id_seq'::regclass);


--
-- Name: meter_bulk_edit_log oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_bulk_edit_log ALTER COLUMN oid SET DEFAULT nextval('public.meter_bulk_edit_log_oid_seq'::regclass);


--
-- Name: meter_group oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_group ALTER COLUMN oid SET DEFAULT nextval('public.meter_group_oid_seq'::regclass);


--
-- Name: meter_group_item oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_group_item ALTER COLUMN oid SET DEFAULT nextval('public.meter_group_item_oid_seq'::regclass);


--
-- Name: meter_message oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_message ALTER COLUMN oid SET DEFAULT nextval('public.meter_message_oid_seq'::regclass);


--
-- Name: meter_reading oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_reading ALTER COLUMN oid SET DEFAULT nextval('public.meter_reading_seq'::regclass);


--
-- Name: mv_assessment_timeseries oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_assessment_timeseries ALTER COLUMN oid SET DEFAULT nextval('public.mv_assessment_timeseries_oid_seq'::regclass);


--
-- Name: mv_baseline_timeseries oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_baseline_timeseries ALTER COLUMN oid SET DEFAULT nextval('public.mv_baseline_timeseries_oid_seq'::regclass);


--
-- Name: mv_exogenous_factor_timeseries oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_exogenous_factor_timeseries ALTER COLUMN oid SET DEFAULT nextval('public.mv_exogenous_factor_timeseries_oid_seq'::regclass);


--
-- Name: mv_model_fit_statistic oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_model_fit_statistic ALTER COLUMN oid SET DEFAULT nextval('public.mv_model_fit_statistic_oid_seq'::regclass);


--
-- Name: mv_nonroutine_event oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_nonroutine_event ALTER COLUMN oid SET DEFAULT nextval('public.mv_nonroutine_event_oid_seq'::regclass);


--
-- Name: mv_program oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program ALTER COLUMN oid SET DEFAULT nextval('public.mv_program_oid_seq'::regclass);


--
-- Name: mv_program_cross_type oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program_cross_type ALTER COLUMN oid SET DEFAULT nextval('public.mv_program_cross_type_oid_seq'::regclass);


--
-- Name: mv_program_type oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program_type ALTER COLUMN oid SET DEFAULT nextval('public.mv_program_type_oid_seq'::regclass);


--
-- Name: mv_project oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project ALTER COLUMN oid SET DEFAULT nextval('public.mv_project_oid_seq'::regclass);


--
-- Name: obvius_meter oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.obvius_meter ALTER COLUMN oid SET DEFAULT nextval('public.obvius_meter_oid_seq'::regclass);


--
-- Name: pge_credential oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.pge_credential ALTER COLUMN oid SET DEFAULT nextval('public.pge_credential_oid_seq'::regclass);


--
-- Name: report oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.report ALTER COLUMN oid SET DEFAULT nextval('public.report_oid_seq'::regclass);


--
-- Name: rtm_monitor meter; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rtm_monitor ALTER COLUMN meter SET DEFAULT nextval('public.rtm_monitor_meter_seq'::regclass);


--
-- Name: sce_gb_customer_account oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_customer_account ALTER COLUMN oid SET DEFAULT nextval('public.sce_gb_customer_account_oid_seq'::regclass);


--
-- Name: sce_gb_customer_agreement oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_customer_agreement ALTER COLUMN oid SET DEFAULT nextval('public.sce_gb_customer_agreement_oid_seq'::regclass);


--
-- Name: sce_gb_resource oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_resource ALTER COLUMN oid SET DEFAULT nextval('public.sce_gb_resource_oid_seq'::regclass);


--
-- Name: sce_gb_retail_customer oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_retail_customer ALTER COLUMN oid SET DEFAULT nextval('public.sce_gb_retail_customer_oid_seq'::regclass);


--
-- Name: scrooge_ops_audit oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.scrooge_ops_audit ALTER COLUMN oid SET DEFAULT nextval('public.scrooge_ops_audit_oid_seq'::regclass);


--
-- Name: smd_authorization_audit oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit ALTER COLUMN oid SET DEFAULT nextval('public.smd_authorization_audit_oid_seq'::regclass);


--
-- Name: smd_authorization_audit_point oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit_point ALTER COLUMN oid SET DEFAULT nextval('public.smd_authorization_audit_point_oid_seq'::regclass);


--
-- Name: smd_subscription_detail oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_subscription_detail ALTER COLUMN oid SET DEFAULT nextval('public.smd_subscription_detail_oid_seq'::regclass);


--
-- Name: snapmeter_account oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_account_oid_seq'::regclass);


--
-- Name: snapmeter_account_data_source oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_data_source ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_account_data_source_oid_seq'::regclass);


--
-- Name: snapmeter_account_meter oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_meter ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_account_meter_oid_seq'::regclass);


--
-- Name: snapmeter_account_user oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_user ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_account_user_oid_seq'::regclass);


--
-- Name: snapmeter_announcement oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_announcement ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_announcement_oid_seq'::regclass);


--
-- Name: snapmeter_data_gap oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_data_gap ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_data_gap_oid_seq'::regclass);


--
-- Name: snapmeter_data_quality_snapshot oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_data_quality_snapshot ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_data_quality_snapshot_oid_seq'::regclass);


--
-- Name: snapmeter_failed_login oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_failed_login ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_failed_login_oid_seq'::regclass);


--
-- Name: snapmeter_image oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_image ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_image_oid_seq'::regclass);


--
-- Name: snapmeter_meter_data_source oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_meter_data_source ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_meter_data_source_oid_seq'::regclass);


--
-- Name: snapmeter_provisioning oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_provisioning_oid_seq'::regclass);


--
-- Name: snapmeter_provisioning_credential oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_credential ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_provisioning_credential_oid_seq'::regclass);


--
-- Name: snapmeter_provisioning_event oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_event ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_provisioning_event_oid_seq'::regclass);


--
-- Name: snapmeter_provisioning_workflow oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_workflow ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_provisioning_workflow_oid_seq'::regclass);


--
-- Name: snapmeter_user oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_user ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_user_oid_seq'::regclass);


--
-- Name: snapmeter_user_subscription oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_user_subscription ALTER COLUMN oid SET DEFAULT nextval('public.snapmeter_user_subscription_oid_seq'::regclass);


--
-- Name: utility oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility ALTER COLUMN oid SET DEFAULT nextval('public.utility_oid_seq'::regclass);


--
-- Name: utility_tariff oid; Type: DEFAULT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility_tariff ALTER COLUMN oid SET DEFAULT nextval('public.utility_tariff_oid_seq'::regclass);


--
-- Name: access_token access_token_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.access_token
    ADD CONSTRAINT access_token_pkey PRIMARY KEY (oid);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: analytic_identifier analytic_identifier_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.analytic_identifier
    ADD CONSTRAINT analytic_identifier_pkey PRIMARY KEY (oid);


--
-- Name: analytic_identifier analytic_identifier_unique_analyitc_source_stored; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.analytic_identifier
    ADD CONSTRAINT analytic_identifier_unique_analyitc_source_stored UNIQUE (analytic, source, stored);


--
-- Name: analytic_run analytic_run_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.analytic_run
    ADD CONSTRAINT analytic_run_pkey PRIMARY KEY (oid);


--
-- Name: archive_fragment archive_fragment_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.archive_fragment
    ADD CONSTRAINT archive_fragment_pkey PRIMARY KEY (oid);


--
-- Name: auth_session auth_session_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.auth_session
    ADD CONSTRAINT auth_session_pkey PRIMARY KEY (id);


--
-- Name: auth_user auth_user_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.auth_user
    ADD CONSTRAINT auth_user_pkey PRIMARY KEY (id);


--
-- Name: average_load_analytics average_load_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.average_load_analytics
    ADD CONSTRAINT average_load_analytics_pkey PRIMARY KEY (oid);


--
-- Name: balance_point_analytics balance_point_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.balance_point_analytics
    ADD CONSTRAINT balance_point_analytics_pkey PRIMARY KEY (oid);


--
-- Name: balance_point_detail balance_point_detail_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.balance_point_detail
    ADD CONSTRAINT balance_point_detail_pkey PRIMARY KEY (oid);


--
-- Name: balance_point_summary balance_point_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.balance_point_summary
    ADD CONSTRAINT balance_point_summary_pkey PRIMARY KEY (oid);


--
-- Name: bill_audit_event bill_audit_event_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill_audit_event
    ADD CONSTRAINT bill_audit_event_pkey PRIMARY KEY (oid);


--
-- Name: bill_audit bill_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill_audit
    ADD CONSTRAINT bill_audit_pkey PRIMARY KEY (oid);


--
-- Name: bill_old bill_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill_old
    ADD CONSTRAINT bill_pkey PRIMARY KEY (oid);


--
-- Name: bill bill_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.bill
    ADD CONSTRAINT bill_v2_pkey PRIMARY KEY (oid);


--
-- Name: budget_aggregation budget_aggregation_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.budget_aggregation
    ADD CONSTRAINT budget_aggregation_pkey PRIMARY KEY (oid);


--
-- Name: building_calendar building_calendar_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.building_calendar
    ADD CONSTRAINT building_calendar_pkey PRIMARY KEY (oid);


--
-- Name: building_occupancy building_occupancy_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.building_occupancy
    ADD CONSTRAINT building_occupancy_pkey PRIMARY KEY (oid);


--
-- Name: building building_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.building
    ADD CONSTRAINT building_pkey PRIMARY KEY (oid);


--
-- Name: cluster_data cluster_data_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.cluster_data
    ADD CONSTRAINT cluster_data_pkey PRIMARY KEY (oid);


--
-- Name: compare_day compare_day_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.compare_day
    ADD CONSTRAINT compare_day_pkey PRIMARY KEY (oid);


--
-- Name: computed_billing_cycle computed_billing_cycle_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.computed_billing_cycle
    ADD CONSTRAINT computed_billing_cycle_pkey PRIMARY KEY (oid);


--
-- Name: configuration configuration_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.configuration
    ADD CONSTRAINT configuration_pkey PRIMARY KEY (application);


--
-- Name: curtailment_peak curtailment_peak_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.curtailment_peak
    ADD CONSTRAINT curtailment_peak_pkey PRIMARY KEY (oid);


--
-- Name: curtailment_recommendation curtailment_recommendation_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.curtailment_recommendation
    ADD CONSTRAINT curtailment_recommendation_pkey PRIMARY KEY (oid);


--
-- Name: daily_budget_forecast daily_budget_forecast_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.daily_budget_forecast
    ADD CONSTRAINT daily_budget_forecast_pkey PRIMARY KEY (oid);


--
-- Name: daily_fact daily_fact_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.daily_fact
    ADD CONSTRAINT daily_fact_pkey PRIMARY KEY (oid);


--
-- Name: daily_trend daily_trend_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.daily_trend
    ADD CONSTRAINT daily_trend_pkey PRIMARY KEY (oid);


--
-- Name: database_archive database_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.database_archive
    ADD CONSTRAINT database_archive_pkey PRIMARY KEY (oid);


--
-- Name: day_cluster_analytics day_cluster_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.day_cluster_analytics
    ADD CONSTRAINT day_cluster_analytics_pkey PRIMARY KEY (oid);


--
-- Name: decomp_facts decomp_facts_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.decomp_facts
    ADD CONSTRAINT decomp_facts_pkey PRIMARY KEY (oid);


--
-- Name: decomposition_data decomposition_data_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.decomposition_data
    ADD CONSTRAINT decomposition_data_pkey PRIMARY KEY (oid);


--
-- Name: decomposition_data decompostion_data_meter_occurred_unique; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.decomposition_data
    ADD CONSTRAINT decompostion_data_meter_occurred_unique UNIQUE (meter, occurred);


--
-- Name: degree_day degree_day_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.degree_day
    ADD CONSTRAINT degree_day_pkey PRIMARY KEY (oid);


--
-- Name: drift_report drift_report_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.drift_report
    ADD CONSTRAINT drift_report_pkey PRIMARY KEY (oid);


--
-- Name: employee employee_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.employee
    ADD CONSTRAINT employee_pkey PRIMARY KEY (oid);


--
-- Name: entry entry_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.entry
    ADD CONSTRAINT entry_pkey PRIMARY KEY (oid);


--
-- Name: error_model error_model_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.error_model
    ADD CONSTRAINT error_model_pkey PRIMARY KEY (oid);


--
-- Name: event event_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.event
    ADD CONSTRAINT event_pkey PRIMARY KEY (oid);


--
-- Name: event_status event_status_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.event_status
    ADD CONSTRAINT event_status_pkey PRIMARY KEY (oid);


--
-- Name: fit_dr_model_data fit_dr_model_data_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.fit_dr_model_data
    ADD CONSTRAINT fit_dr_model_data_pkey PRIMARY KEY (oid);


--
-- Name: fit_dr_model_summary fit_dr_model_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.fit_dr_model_summary
    ADD CONSTRAINT fit_dr_model_summary_pkey PRIMARY KEY (oid);


--
-- Name: forecast_dr_evaluation forecast_dr_evaluation_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.forecast_dr_evaluation
    ADD CONSTRAINT forecast_dr_evaluation_pkey PRIMARY KEY (oid);


--
-- Name: forecast_location forecast_location_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.forecast_location
    ADD CONSTRAINT forecast_location_pkey PRIMARY KEY (oid);


--
-- Name: forecast_model_stats forecast_model_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.forecast_model_stats
    ADD CONSTRAINT forecast_model_stats_pkey PRIMARY KEY (oid);


--
-- Name: foreign_system_account foreign_system_account_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.foreign_system_account
    ADD CONSTRAINT foreign_system_account_pkey PRIMARY KEY (oid);


--
-- Name: foreign_system_attribute foreign_system_attribute_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.foreign_system_attribute
    ADD CONSTRAINT foreign_system_attribute_pkey PRIMARY KEY (oid);


--
-- Name: green_button_customer_account green_button_customer_account_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_customer_account
    ADD CONSTRAINT green_button_customer_account_pkey PRIMARY KEY (oid);


--
-- Name: green_button_customer_agreement green_button_customer_agreement_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_customer_agreement
    ADD CONSTRAINT green_button_customer_agreement_pkey PRIMARY KEY (oid);


--
-- Name: green_button_customer green_button_customer_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_customer
    ADD CONSTRAINT green_button_customer_pkey PRIMARY KEY (oid);


--
-- Name: green_button_gap_fill_job green_button_gap_fill_job_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_gap_fill_job
    ADD CONSTRAINT green_button_gap_fill_job_pkey PRIMARY KEY (oid);


--
-- Name: green_button_interval_block green_button_interval_block_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_interval_block
    ADD CONSTRAINT green_button_interval_block_pkey PRIMARY KEY (oid);


--
-- Name: green_button_meter_reading green_button_meter_reading_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_meter_reading
    ADD CONSTRAINT green_button_meter_reading_pkey PRIMARY KEY (oid);


--
-- Name: green_button_notification green_button_notification_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification
    ADD CONSTRAINT green_button_notification_pkey PRIMARY KEY (oid);


--
-- Name: green_button_notification_resource green_button_notification_resource_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_resource
    ADD CONSTRAINT green_button_notification_resource_pkey PRIMARY KEY (oid);


--
-- Name: green_button_notification_task green_button_notification_task_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_task
    ADD CONSTRAINT green_button_notification_task_pkey PRIMARY KEY (oid);


--
-- Name: green_button_provider green_button_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_provider
    ADD CONSTRAINT green_button_provider_pkey PRIMARY KEY (oid);


--
-- Name: green_button_reading_stats green_button_reading_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_reading_stats
    ADD CONSTRAINT green_button_reading_stats_pkey PRIMARY KEY (oid);


--
-- Name: green_button_reading_type green_button_reading_type_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_reading_type
    ADD CONSTRAINT green_button_reading_type_pkey PRIMARY KEY (oid);


--
-- Name: green_button_retail_customer green_button_retail_customer_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_retail_customer
    ADD CONSTRAINT green_button_retail_customer_pkey PRIMARY KEY (oid);


--
-- Name: green_button_subscription_task green_button_subscription_task_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_subscription_task
    ADD CONSTRAINT green_button_subscription_task_pkey PRIMARY KEY (oid);


--
-- Name: green_button_task green_button_task_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_task
    ADD CONSTRAINT green_button_task_pkey PRIMARY KEY (oid);


--
-- Name: green_button_time_parameters green_button_time_parameters_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_time_parameters
    ADD CONSTRAINT green_button_time_parameters_pkey PRIMARY KEY (oid);


--
-- Name: green_button_usage_point green_button_usage_point_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_usage_point
    ADD CONSTRAINT green_button_usage_point_pkey PRIMARY KEY (oid);


--
-- Name: green_button_usage_summary green_button_usage_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_usage_summary
    ADD CONSTRAINT green_button_usage_summary_pkey PRIMARY KEY (oid);


--
-- Name: hours_at_demand hours_at_demand_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.hours_at_demand
    ADD CONSTRAINT hours_at_demand_pkey PRIMARY KEY (oid);


--
-- Name: integration_test_run integration_test_run_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.integration_test_run
    ADD CONSTRAINT integration_test_run_pkey PRIMARY KEY (oid);


--
-- Name: interval_facts interval_facts_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.interval_facts
    ADD CONSTRAINT interval_facts_pkey PRIMARY KEY (oid);


--
-- Name: latest_snapmeter latest_snapmeter_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.latest_snapmeter
    ADD CONSTRAINT latest_snapmeter_pkey PRIMARY KEY (oid);


--
-- Name: load_analytics load_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.load_analytics
    ADD CONSTRAINT load_analytics_pkey PRIMARY KEY (oid);


--
-- Name: load_by_day load_by_day_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.load_by_day
    ADD CONSTRAINT load_by_day_pkey PRIMARY KEY (oid);


--
-- Name: load_duration load_duration_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.load_duration
    ADD CONSTRAINT load_duration_pkey PRIMARY KEY (oid);


--
-- Name: message_template message_template_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.message_template
    ADD CONSTRAINT message_template_pkey PRIMARY KEY (oid);


--
-- Name: messaging_email_event messaging_email_events_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_email_event
    ADD CONSTRAINT messaging_email_events_pkey PRIMARY KEY (id);


--
-- Name: messaging_email messaging_email_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_email
    ADD CONSTRAINT messaging_email_pkey PRIMARY KEY (id);


--
-- Name: messaging_incoming_email messaging_incoming_email_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_incoming_email
    ADD CONSTRAINT messaging_incoming_email_pkey PRIMARY KEY (id);


--
-- Name: messaging_sms_incoming_message messaging_sms_incoming_message_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_incoming_message
    ADD CONSTRAINT messaging_sms_incoming_message_pkey PRIMARY KEY (id);


--
-- Name: messaging_sms_message_event messaging_sms_message_event_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_message_event
    ADD CONSTRAINT messaging_sms_message_event_pkey PRIMARY KEY (id);


--
-- Name: messaging_sms_message messaging_sms_message_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_message
    ADD CONSTRAINT messaging_sms_message_pkey PRIMARY KEY (id);


--
-- Name: meter_analytics meter_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_analytics
    ADD CONSTRAINT meter_analytics_pkey PRIMARY KEY (oid);


--
-- Name: meter_analytics meter_analytics_unique_meter_period; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_analytics
    ADD CONSTRAINT meter_analytics_unique_meter_period UNIQUE (meter, period);


--
-- Name: meter_bulk_edit_log meter_bulk_edit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_bulk_edit_log
    ADD CONSTRAINT meter_bulk_edit_log_pkey PRIMARY KEY (oid);


--
-- Name: meter_group_item meter_group_item_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_group_item
    ADD CONSTRAINT meter_group_item_pkey PRIMARY KEY (oid);


--
-- Name: meter_group meter_group_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_group
    ADD CONSTRAINT meter_group_pkey PRIMARY KEY (oid);


--
-- Name: meter_message meter_message_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_message
    ADD CONSTRAINT meter_message_pkey PRIMARY KEY (oid);


--
-- Name: meter meter_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter
    ADD CONSTRAINT meter_pkey PRIMARY KEY (oid);


--
-- Name: meter_reading meter_reading_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_reading
    ADD CONSTRAINT meter_reading_pkey PRIMARY KEY (oid);


--
-- Name: model_statistic model_statistic_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.model_statistic
    ADD CONSTRAINT model_statistic_pkey PRIMARY KEY (oid);


--
-- Name: monthly_budget_forecast monthly_budget_forecast_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.monthly_budget_forecast
    ADD CONSTRAINT monthly_budget_forecast_pkey PRIMARY KEY (oid);


--
-- Name: monthly_fact monthly_fact_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.monthly_fact
    ADD CONSTRAINT monthly_fact_pkey PRIMARY KEY (oid);


--
-- Name: monthly_forecast monthly_forecast_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.monthly_forecast
    ADD CONSTRAINT monthly_forecast_pkey PRIMARY KEY (oid);


--
-- Name: monthly_yoy_variance monthly_yoy_variance_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.monthly_yoy_variance
    ADD CONSTRAINT monthly_yoy_variance_pkey PRIMARY KEY (oid);


--
-- Name: mv_assessment_timeseries mv_assessment_timeseries_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_assessment_timeseries
    ADD CONSTRAINT mv_assessment_timeseries_pkey PRIMARY KEY (oid);


--
-- Name: mv_baseline mv_baseline_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_baseline
    ADD CONSTRAINT mv_baseline_pkey PRIMARY KEY (oid);


--
-- Name: mv_baseline_timeseries mv_baseline_timeseries_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_baseline_timeseries
    ADD CONSTRAINT mv_baseline_timeseries_pkey PRIMARY KEY (oid);


--
-- Name: mv_drift_data mv_drift_data_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_drift_data
    ADD CONSTRAINT mv_drift_data_pkey PRIMARY KEY (oid);


--
-- Name: mv_drift_period mv_drift_period_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_drift_period
    ADD CONSTRAINT mv_drift_period_pkey PRIMARY KEY (oid);


--
-- Name: mv_exogenous_factor_timeseries mv_exogenous_factor_timeseries_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_exogenous_factor_timeseries
    ADD CONSTRAINT mv_exogenous_factor_timeseries_pkey PRIMARY KEY (oid);


--
-- Name: mv_model_fit_statistic mv_model_fit_statistic_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_model_fit_statistic
    ADD CONSTRAINT mv_model_fit_statistic_pkey PRIMARY KEY (oid);


--
-- Name: mv_nonroutine_event mv_nonroutine_event_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_nonroutine_event
    ADD CONSTRAINT mv_nonroutine_event_pkey PRIMARY KEY (oid);


--
-- Name: mv_program_cross_type mv_program_cross_type_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program_cross_type
    ADD CONSTRAINT mv_program_cross_type_pkey PRIMARY KEY (oid);


--
-- Name: mv_program mv_program_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program
    ADD CONSTRAINT mv_program_pkey PRIMARY KEY (oid);


--
-- Name: mv_program_type mv_program_type_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program_type
    ADD CONSTRAINT mv_program_type_pkey PRIMARY KEY (oid);


--
-- Name: mv_project mv_project_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project
    ADD CONSTRAINT mv_project_pkey PRIMARY KEY (oid);


--
-- Name: obvius_meter obvius_meter_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.obvius_meter
    ADD CONSTRAINT obvius_meter_pkey PRIMARY KEY (oid);


--
-- Name: pdp_analytics pdp_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.pdp_analytics
    ADD CONSTRAINT pdp_analytics_pkey PRIMARY KEY (oid);


--
-- Name: pdp_event_interval pdp_event_interval_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.pdp_event_interval
    ADD CONSTRAINT pdp_event_interval_pkey PRIMARY KEY (event, meter);


--
-- Name: pdp_event pdp_event_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.pdp_event
    ADD CONSTRAINT pdp_event_pkey PRIMARY KEY (event, meter);


--
-- Name: pdp_event_weather pdp_event_weather_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.pdp_event_weather
    ADD CONSTRAINT pdp_event_weather_pkey PRIMARY KEY (event, station);


--
-- Name: peak_billing_fact peak_billing_fact_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_billing_fact
    ADD CONSTRAINT peak_billing_fact_pkey PRIMARY KEY (oid);


--
-- Name: peak_forecast peak_forecast_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_forecast
    ADD CONSTRAINT peak_forecast_pkey PRIMARY KEY (oid);


--
-- Name: peak_history peak_history_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_history
    ADD CONSTRAINT peak_history_pkey PRIMARY KEY (oid);


--
-- Name: peak_prediction peak_prediction_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_prediction
    ADD CONSTRAINT peak_prediction_pkey PRIMARY KEY (oid);


--
-- Name: pge_credential pge_credential_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.pge_credential
    ADD CONSTRAINT pge_credential_pkey PRIMARY KEY (oid);


--
-- Name: plotting_fact plotting_fact_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.plotting_fact
    ADD CONSTRAINT plotting_fact_pkey PRIMARY KEY (oid);


--
-- Name: product_enrollment product_enrollment_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.product_enrollment
    ADD CONSTRAINT product_enrollment_pkey PRIMARY KEY (oid);


--
-- Name: provision_entry provision_entry_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.provision_entry
    ADD CONSTRAINT provision_entry_pkey PRIMARY KEY (oid);


--
-- Name: provision_extract provision_extract_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.provision_extract
    ADD CONSTRAINT provision_extract_pkey PRIMARY KEY (oid);


--
-- Name: rate_analysis rate_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rate_analysis
    ADD CONSTRAINT rate_analysis_pkey PRIMARY KEY (oid);


--
-- Name: rate_model_coefficient rate_model_coefficient_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rate_model_coefficient
    ADD CONSTRAINT rate_model_coefficient_pkey PRIMARY KEY (oid);


--
-- Name: rate_right rate_right_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rate_right
    ADD CONSTRAINT rate_right_pkey PRIMARY KEY (oid);


--
-- Name: rate_right_summary rate_right_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rate_right_summary
    ADD CONSTRAINT rate_right_summary_pkey PRIMARY KEY (oid);


--
-- Name: rcx_pattern_analytics rcx_pattern_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rcx_pattern_analytics
    ADD CONSTRAINT rcx_pattern_analytics_pkey PRIMARY KEY (oid);


--
-- Name: real_time_attribute real_time_attribute_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.real_time_attribute
    ADD CONSTRAINT real_time_attribute_pkey PRIMARY KEY (oid);


--
-- Name: recommendation_indices recommendation_indices_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.recommendation_indices
    ADD CONSTRAINT recommendation_indices_pkey PRIMARY KEY (oid);


--
-- Name: report report_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.report
    ADD CONSTRAINT report_pkey PRIMARY KEY (oid);


--
-- Name: rtm_monitor rtm_monitor_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.rtm_monitor
    ADD CONSTRAINT rtm_monitor_pkey PRIMARY KEY (meter);


--
-- Name: savings_estimates savings_estimates_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.savings_estimates
    ADD CONSTRAINT savings_estimates_pkey PRIMARY KEY (oid);


--
-- Name: sce_gb_customer_account sce_gb_customer_account_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_customer_account
    ADD CONSTRAINT sce_gb_customer_account_pkey PRIMARY KEY (oid);


--
-- Name: sce_gb_customer_agreement sce_gb_customer_agreement_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_customer_agreement
    ADD CONSTRAINT sce_gb_customer_agreement_pkey PRIMARY KEY (oid);


--
-- Name: sce_gb_resource sce_gb_resource_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_resource
    ADD CONSTRAINT sce_gb_resource_pkey PRIMARY KEY (oid);


--
-- Name: sce_gb_retail_customer sce_gb_retail_customer_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_retail_customer
    ADD CONSTRAINT sce_gb_retail_customer_pkey PRIMARY KEY (oid);


--
-- Name: score_pattern_analytics score_pattern_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.score_pattern_analytics
    ADD CONSTRAINT score_pattern_analytics_pkey PRIMARY KEY (oid);


--
-- Name: scrooge_ops_audit scrooge_ops_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.scrooge_ops_audit
    ADD CONSTRAINT scrooge_ops_audit_pkey PRIMARY KEY (oid);


--
-- Name: smd_authorization_audit smd_authorization_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit
    ADD CONSTRAINT smd_authorization_audit_pkey PRIMARY KEY (oid);


--
-- Name: smd_authorization_audit_point smd_authorization_audit_point_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit_point
    ADD CONSTRAINT smd_authorization_audit_point_pkey PRIMARY KEY (oid);


--
-- Name: smd_subscription_detail smd_subscription_detail_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_subscription_detail
    ADD CONSTRAINT smd_subscription_detail_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_account_data_source snapmeter_account_data_source_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_data_source
    ADD CONSTRAINT snapmeter_account_data_source_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_account snapmeter_account_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account
    ADD CONSTRAINT snapmeter_account_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_account_user snapmeter_account_user_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_user
    ADD CONSTRAINT snapmeter_account_user_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_announcement snapmeter_announcement_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_announcement
    ADD CONSTRAINT snapmeter_announcement_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_building snapmeter_building_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_building
    ADD CONSTRAINT snapmeter_building_pkey PRIMARY KEY (building, account);


--
-- Name: snapmeter_data_gap snapmeter_data_gap_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_data_gap
    ADD CONSTRAINT snapmeter_data_gap_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_data_quality_snapshot snapmeter_data_quality_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_data_quality_snapshot
    ADD CONSTRAINT snapmeter_data_quality_snapshot_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_failed_login snapmeter_failed_login_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_failed_login
    ADD CONSTRAINT snapmeter_failed_login_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_image snapmeter_image_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_image
    ADD CONSTRAINT snapmeter_image_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_meter_data_source snapmeter_meter_data_source_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_meter_data_source
    ADD CONSTRAINT snapmeter_meter_data_source_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_provisioning_credential snapmeter_provisioning_credential_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_credential
    ADD CONSTRAINT snapmeter_provisioning_credential_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_provisioning_event snapmeter_provisioning_event_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_event
    ADD CONSTRAINT snapmeter_provisioning_event_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_provisioning snapmeter_provisioning_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning
    ADD CONSTRAINT snapmeter_provisioning_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_provisioning_workflow snapmeter_provisioning_workflow_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_workflow
    ADD CONSTRAINT snapmeter_provisioning_workflow_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_user snapmeter_user_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_user
    ADD CONSTRAINT snapmeter_user_pkey PRIMARY KEY (oid);


--
-- Name: snapmeter_user_subscription snapmeter_user_subscription_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_user_subscription
    ADD CONSTRAINT snapmeter_user_subscription_pkey PRIMARY KEY (oid);


--
-- Name: standard_holiday standard_holiday_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.standard_holiday
    ADD CONSTRAINT standard_holiday_pkey PRIMARY KEY (oid);


--
-- Name: stasis stasis_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.stasis
    ADD CONSTRAINT stasis_pkey PRIMARY KEY (oid);


--
-- Name: stasis_transaction stasis_transaction_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.stasis_transaction
    ADD CONSTRAINT stasis_transaction_pkey PRIMARY KEY (oid);


--
-- Name: temperature_response temperature_response_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.temperature_response
    ADD CONSTRAINT temperature_response_pkey PRIMARY KEY (oid);


--
-- Name: test_stored test_stored_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.test_stored
    ADD CONSTRAINT test_stored_pkey PRIMARY KEY (oid);


--
-- Name: trailing_twelve_month_analytics trailing_twelve_month_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.trailing_twelve_month_analytics
    ADD CONSTRAINT trailing_twelve_month_analytics_pkey PRIMARY KEY (oid);


--
-- Name: ttm_calculation ttm_calculation_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_calculation
    ADD CONSTRAINT ttm_calculation_pkey PRIMARY KEY (oid);


--
-- Name: ttm_fact ttm_fact_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_fact
    ADD CONSTRAINT ttm_fact_pkey PRIMARY KEY (oid);


--
-- Name: ttm_period ttm_period_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_period
    ADD CONSTRAINT ttm_period_pkey PRIMARY KEY (oid);


--
-- Name: balance_point_summary unique_balance_point_summary_meter_model_type; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.balance_point_summary
    ADD CONSTRAINT unique_balance_point_summary_meter_model_type UNIQUE (meter, period_type);


--
-- Name: budget_aggregation unique_budget_aggregation_cycle; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.budget_aggregation
    ADD CONSTRAINT unique_budget_aggregation_cycle UNIQUE (cycle);


--
-- Name: daily_fact unique_daily_fact_meter_date; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.daily_fact
    ADD CONSTRAINT unique_daily_fact_meter_date UNIQUE (meter, date);


--
-- Name: interval_facts unique_interval_facts_daily_time; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.interval_facts
    ADD CONSTRAINT unique_interval_facts_daily_time UNIQUE (daily, "time");


--
-- Name: peak_forecast unique_peak_forecast_meter_date; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_forecast
    ADD CONSTRAINT unique_peak_forecast_meter_date UNIQUE (meter, date, tou);


--
-- Name: peak_history unique_peak_history_meter_date; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_history
    ADD CONSTRAINT unique_peak_history_meter_date UNIQUE (meter, date, tou);


--
-- Name: peak_prediction unique_peak_prediction_meter_occurred; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.peak_prediction
    ADD CONSTRAINT unique_peak_prediction_meter_occurred UNIQUE (meter, occurred);


--
-- Name: ttm_fact unique_ttm_fact_period_variable; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_fact
    ADD CONSTRAINT unique_ttm_fact_period_variable UNIQUE (period, variable);


--
-- Name: ttm_period unique_ttm_period_meter_year_start_year_end; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_period
    ADD CONSTRAINT unique_ttm_period_meter_year_start_year_end UNIQUE (meter, year_start, year_end);


--
-- Name: usage_history unique_usage_history_meter_occurred; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.usage_history
    ADD CONSTRAINT unique_usage_history_meter_occurred UNIQUE (meter, occurred);


--
-- Name: use_prediction unique_use_prediction_meter_occurred; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.use_prediction
    ADD CONSTRAINT unique_use_prediction_meter_occurred UNIQUE (meter, occurred);


--
-- Name: variance_clause unique_variance_clause_analysis; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.variance_clause
    ADD CONSTRAINT unique_variance_clause_analysis UNIQUE (analysis);


--
-- Name: variance_clause unique_variance_clause_baseline; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.variance_clause
    ADD CONSTRAINT unique_variance_clause_baseline UNIQUE (baseline);


--
-- Name: usage_history usage_history_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.usage_history
    ADD CONSTRAINT usage_history_pkey PRIMARY KEY (oid);


--
-- Name: use_prediction use_prediction_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.use_prediction
    ADD CONSTRAINT use_prediction_pkey PRIMARY KEY (oid);


--
-- Name: utility_account utility_account_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility_account
    ADD CONSTRAINT utility_account_pkey PRIMARY KEY (oid);


--
-- Name: utility utility_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility
    ADD CONSTRAINT utility_pkey PRIMARY KEY (oid);


--
-- Name: utility_service utility_service_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility_service
    ADD CONSTRAINT utility_service_pkey PRIMARY KEY (oid);


--
-- Name: utility_tariff utility_tariff_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility_tariff
    ADD CONSTRAINT utility_tariff_pkey PRIMARY KEY (oid);


--
-- Name: variance_clause variance_clause_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.variance_clause
    ADD CONSTRAINT variance_clause_pkey PRIMARY KEY (oid);


--
-- Name: weather_forecast weather_forecast_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.weather_forecast
    ADD CONSTRAINT weather_forecast_pkey PRIMARY KEY (oid);


--
-- Name: weather_history_log weather_history_log_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.weather_history_log
    ADD CONSTRAINT weather_history_log_pkey PRIMARY KEY (oid);


--
-- Name: weather_history weather_history_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.weather_history
    ADD CONSTRAINT weather_history_pkey PRIMARY KEY (oid);


--
-- Name: weather_source_axis weather_source_axis_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.weather_source_axis
    ADD CONSTRAINT weather_source_axis_pkey PRIMARY KEY (oid);


--
-- Name: weather_station weather_station_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.weather_station
    ADD CONSTRAINT weather_station_pkey PRIMARY KEY (oid);


--
-- Name: workflow_task_run workflow_task_run_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.workflow_task_run
    ADD CONSTRAINT workflow_task_run_pkey PRIMARY KEY (oid);


--
-- Name: wsi_axis wsi_axis_pkey; Type: CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.wsi_axis
    ADD CONSTRAINT wsi_axis_pkey PRIMARY KEY (oid);


--
-- Name: account_user_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX account_user_unique ON public.snapmeter_account_user USING btree ("user", account);


--
-- Name: analytic_run_meter_analytics; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX analytic_run_meter_analytics ON public.analytic_run USING btree (meter, analytics);


--
-- Name: analytic_run_status; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX analytic_run_status ON public.analytic_run USING btree (status);


--
-- Name: analytic_run_uuid; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX analytic_run_uuid ON public.analytic_run USING btree (uuid);


--
-- Name: archive_fragment_archive; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX archive_fragment_archive ON public.archive_fragment USING btree (archive);


--
-- Name: archive_fragment_archive_identity; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX archive_fragment_archive_identity ON public.archive_fragment USING btree (archive, identity);


--
-- Name: average_load_analytics_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX average_load_analytics_meter ON public.average_load_analytics USING btree (meter);


--
-- Name: balance_point_analytics_meter_day; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX balance_point_analytics_meter_day ON public.balance_point_analytics USING btree (meter, day);


--
-- Name: balance_point_summary_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX balance_point_summary_meter ON public.balance_point_summary USING btree (meter);


--
-- Name: bill_audit_complete; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX bill_audit_complete ON public.bill_old USING btree (audit_complete);


--
-- Name: bill_failed_audits; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX bill_failed_audits ON public.bill_old USING btree (audit_complete, audit_successful, audit_suppressed, audit_accepted);


--
-- Name: bill_service_closing_initial; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX bill_service_closing_initial ON public.bill_old USING btree (service, closing, initial, cost, used);


--
-- Name: bill_service_closing_initial_2; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX bill_service_closing_initial_2 ON public.bill USING btree (service, closing, initial, cost, used);


--
-- Name: bill_service_initial_closing; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX bill_service_initial_closing ON public.bill_old USING btree (service, initial, closing);


--
-- Name: bill_service_initial_closing_2; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX bill_service_initial_closing_2 ON public.bill USING btree (service, initial, closing);


--
-- Name: budget_aggregation_cycle; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX budget_aggregation_cycle ON public.budget_aggregation USING btree (cycle);


--
-- Name: budget_aggregation_cycle_era; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX budget_aggregation_cycle_era ON public.budget_aggregation USING btree (cycle, era);


--
-- Name: building_calendar_building_year; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX building_calendar_building_year ON public.building_calendar USING btree (building, year);


--
-- Name: building_coordinate_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX building_coordinate_index ON public.building USING gist (coordinates);


--
-- Name: building_occupancy_building; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX building_occupancy_building ON public.building_occupancy USING btree (building, month);


--
-- Name: cluster_data_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX cluster_data_meter_occurred ON public.cluster_data USING btree (meter, occurred);


--
-- Name: compare_day_meter_event; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX compare_day_meter_event ON public.compare_day USING btree (meter, event);


--
-- Name: computed_billing_cycle_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX computed_billing_cycle_meter ON public.computed_billing_cycle USING btree (meter);


--
-- Name: curtailment_peak_recommendation; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX curtailment_peak_recommendation ON public.curtailment_peak USING btree (recommendation);


--
-- Name: curtailment_recommendation_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX curtailment_recommendation_meter ON public.curtailment_recommendation USING btree (meter);


--
-- Name: daily_budget_forecast_meter_created; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX daily_budget_forecast_meter_created ON public.daily_budget_forecast USING btree (meter, created);


--
-- Name: daily_budget_forecast_meter_date_type; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX daily_budget_forecast_meter_date_type ON public.daily_budget_forecast USING btree (meter, date, type);


--
-- Name: daily_fact_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX daily_fact_meter ON public.daily_fact USING btree (meter);


--
-- Name: daily_fact_meter_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX daily_fact_meter_date ON public.daily_fact USING btree (meter, date);


--
-- Name: daily_trend_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX daily_trend_meter ON public.daily_trend USING btree (meter);


--
-- Name: day_cluster_analytics_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX day_cluster_analytics_meter ON public.day_cluster_analytics USING btree (meter);


--
-- Name: decomp_facts_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX decomp_facts_meter ON public.decomp_facts USING btree (meter);


--
-- Name: decomposition_data_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX decomposition_data_meter_occurred ON public.decomposition_data USING btree (meter, occurred);


--
-- Name: degree_day_station_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX degree_day_station_date ON public.degree_day USING btree (source, date);


--
-- Name: drift_report_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX drift_report_meter ON public.drift_report USING btree (meter);


--
-- Name: email_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX email_unique ON public.snapmeter_user USING btree (email);


--
-- Name: error_model_run; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX error_model_run ON public.error_model USING btree (run);


--
-- Name: event_status_meter_event; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX event_status_meter_event ON public.event_status USING btree (meter, event);


--
-- Name: fit_dr_model_data_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX fit_dr_model_data_meter ON public.fit_dr_model_data USING btree (meter);


--
-- Name: fit_dr_model_summary_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX fit_dr_model_summary_meter_occurred ON public.fit_dr_model_summary USING btree (meter, occurred);


--
-- Name: forecast_dr_evaluation_meter_variety_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX forecast_dr_evaluation_meter_variety_occurred ON public.forecast_dr_evaluation USING btree (meter, variety, occurred);


--
-- Name: forecast_location_coordinate_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX forecast_location_coordinate_index ON public.forecast_location USING gist (coordinates);


--
-- Name: green_button_customer_account_identifier_customer; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_customer_account_identifier_customer ON public.green_button_customer_account USING btree (identifier, customer);


--
-- Name: green_button_customer_agreement_identifier_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_customer_agreement_identifier_account ON public.green_button_customer_agreement USING btree (identifier, account);


--
-- Name: green_button_customer_identifier_retail; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_customer_identifier_retail ON public.green_button_customer USING btree (identifier, retail);


--
-- Name: green_button_customer_retail; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_customer_retail ON public.green_button_customer USING btree (retail);


--
-- Name: green_button_interval_block_reading_start; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_interval_block_reading_start ON public.green_button_interval_block USING btree (reading, start);


--
-- Name: green_button_meter_reading_identifier_point; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_meter_reading_identifier_point ON public.green_button_meter_reading USING btree (identifier, point);


--
-- Name: green_button_meter_reading_point; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_meter_reading_point ON public.green_button_meter_reading USING btree (point);


--
-- Name: green_button_retail_customer_identifier_provider; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_retail_customer_identifier_provider ON public.green_button_retail_customer USING btree (identifier, provider);


--
-- Name: green_button_retail_customer_provider; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_retail_customer_provider ON public.green_button_retail_customer USING btree (provider);


--
-- Name: green_button_usage_point_identifier_retail; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_usage_point_identifier_retail ON public.green_button_usage_point USING btree (identifier, retail);


--
-- Name: green_button_usage_summary_point_start; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX green_button_usage_summary_point_start ON public.green_button_usage_summary USING btree (point, start);


--
-- Name: hours_at_demand_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX hours_at_demand_meter ON public.hours_at_demand USING btree (meter);


--
-- Name: idx_access_token_token; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_access_token_token ON public.access_token USING btree (token);


--
-- Name: idx_account_ds_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_account_ds_account ON public.snapmeter_account_data_source USING btree (account);


--
-- Name: idx_account_ds_hex; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_account_ds_hex ON public.snapmeter_account_data_source USING btree (hex_id);


--
-- Name: idx_account_hex; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_account_hex ON public.snapmeter_account USING btree (hex_id);


--
-- Name: idx_account_meter_uniq; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_account_meter_uniq ON public.snapmeter_account_meter USING btree (account, meter);


--
-- Name: idx_account_name; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_account_name ON public.snapmeter_account USING btree (name);


--
-- Name: idx_ce_account_oid; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_ce_account_oid ON public.ce_account USING btree (oid);


--
-- Name: idx_event; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_event ON public.compare_day USING btree (event);


--
-- Name: idx_event_chart; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_event_chart ON public.compare_day USING btree (event, chart);


--
-- Name: idx_failed_analytics; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_failed_analytics ON public.analytic_run USING btree (analytics, occurred, status);


--
-- Name: idx_failed_login_email_dt; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_failed_login_email_dt ON public.snapmeter_failed_login USING btree (email, login_dt);


--
-- Name: idx_interval_facts_daily; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_interval_facts_daily ON public.interval_facts USING btree (daily);


--
-- Name: idx_message_template_name; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_message_template_name ON public.message_template USING btree (name);


--
-- Name: idx_meter_bulk_edit_log; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_bulk_edit_log ON public.meter_bulk_edit_log USING btree (meter);


--
-- Name: idx_meter_ds_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_ds_account ON public.snapmeter_meter_data_source USING btree (account_data_source);


--
-- Name: idx_meter_ds_hex; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_ds_hex ON public.snapmeter_meter_data_source USING btree (hex_id);


--
-- Name: idx_meter_ds_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_ds_meter ON public.snapmeter_meter_data_source USING btree (meter);


--
-- Name: idx_meter_gb_usage_point_retail; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_gb_usage_point_retail ON public.green_button_usage_point USING btree (retail);


--
-- Name: idx_meter_message_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_message_account ON public.meter_message USING btree (account_hex, status, reference);


--
-- Name: idx_meter_message_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_message_meter ON public.meter_message USING btree (meter, status, reference);


--
-- Name: idx_meter_message_report; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_message_report ON public.meter_message USING btree (report);


--
-- Name: idx_meter_reading_reading_type; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_reading_reading_type ON public.green_button_meter_reading USING btree (reading_type);


--
-- Name: idx_meter_uncommon_direction; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_meter_uncommon_direction ON public.meter USING btree (direction) WHERE (direction <> 'forward'::public.flow_direction_enum);


--
-- Name: idx_mv_assessment_timeseries_program_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_mv_assessment_timeseries_program_meter_occurred ON public.mv_assessment_timeseries USING btree (program, meter, occurred);


--
-- Name: idx_mv_baseline_timeseries_program_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_mv_baseline_timeseries_program_meter_occurred ON public.mv_baseline_timeseries USING btree (program, meter, occurred);


--
-- Name: idx_mv_exogenous_factor_timeseries_program_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_mv_exogenous_factor_timeseries_program_meter_occurred ON public.mv_exogenous_factor_timeseries USING btree (program, meter, occurred);


--
-- Name: idx_mv_model_fit_statistic_program_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_mv_model_fit_statistic_program_meter ON public.mv_model_fit_statistic USING btree (program, meter);


--
-- Name: idx_mv_nonroutine_event_program_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_mv_nonroutine_event_program_meter_occurred ON public.mv_nonroutine_event USING btree (program, meter, occurred);


--
-- Name: idx_obvius_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_obvius_meter ON public.obvius_meter USING btree (das_serial, channel, meter);


--
-- Name: idx_peak_billing_fact_cycle; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_peak_billing_fact_cycle ON public.peak_billing_fact USING btree (cycle);


--
-- Name: idx_pge_account_ce; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_pge_account_ce ON public.pge_account USING btree (ce);


--
-- Name: idx_pge_bill_key_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_pge_bill_key_unique ON public.pge_bill USING btree (key);


--
-- Name: idx_product_enrollment_product; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_product_enrollment_product ON public.product_enrollment USING btree (product);


--
-- Name: idx_provision_origin_oid; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_provision_origin_oid ON public.provision_origin USING btree (oid);


--
-- Name: idx_rt_attr_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_rt_attr_meter ON public.real_time_attribute USING btree (meter);


--
-- Name: idx_rt_attr_serial_channel; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_rt_attr_serial_channel ON public.real_time_attribute USING btree (das_serial, channel);


--
-- Name: idx_smd_authorization_audit_credential; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_smd_authorization_audit_credential ON public.smd_authorization_audit USING btree (credential);


--
-- Name: idx_smd_authorization_audit_flags; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_smd_authorization_audit_flags ON public.smd_authorization_audit USING btree (completed, authorized, validated_credentials);


--
-- Name: idx_smd_authorization_audit_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_smd_authorization_audit_occurred ON public.smd_authorization_audit USING btree (occurred);


--
-- Name: idx_smd_authorization_audit_point; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_smd_authorization_audit_point ON public.smd_authorization_audit_point USING btree (retail_customer_id, service_id);


--
-- Name: idx_snapmeter_data_gap_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_snapmeter_data_gap_account ON public.snapmeter_data_gap USING btree (account, gap_type);


--
-- Name: idx_snapmeter_data_gap_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_snapmeter_data_gap_meter ON public.snapmeter_data_gap USING btree (meter);


--
-- Name: idx_snapmeter_image_run_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_snapmeter_image_run_date ON public.snapmeter_image USING btree (run_date);


--
-- Name: idx_snapmeter_provisioning_event_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_snapmeter_provisioning_event_occurred ON public.snapmeter_provisioning_event USING btree (workflow, occurred);


--
-- Name: idx_utility_service_tariff_utility; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX idx_utility_service_tariff_utility ON public.utility_service USING btree (tariff, utility);


--
-- Name: idx_utility_tariff_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_utility_tariff_unique ON public.utility_tariff USING btree (utility, tariff);


--
-- Name: idx_utility_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX idx_utility_unique ON public.utility USING btree (identifier);


--
-- Name: interval_facts_daily; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX interval_facts_daily ON public.interval_facts USING btree (daily);


--
-- Name: latest_snapmeter_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX latest_snapmeter_meter ON public.latest_snapmeter USING btree (meter);


--
-- Name: load_by_day_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX load_by_day_meter ON public.load_by_day USING btree (meter);


--
-- Name: load_duration_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX load_duration_meter ON public.load_duration USING btree (meter);


--
-- Name: meter_analytics_meter_period; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_analytics_meter_period ON public.meter_analytics USING btree (meter, period);


--
-- Name: meter_ds_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX meter_ds_unique ON public.snapmeter_meter_data_source USING btree (account_data_source, meter, name);


--
-- Name: meter_group_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_group_account ON public.meter_group USING btree (account);


--
-- Name: meter_group_item_group; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_group_item_group ON public.meter_group_item USING btree ("group");


--
-- Name: meter_point; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_point ON public.meter USING btree (point);


--
-- Name: meter_reading_meter_frozen; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_reading_meter_frozen ON public.meter_reading USING btree (meter, frozen);


--
-- Name: meter_reading_meter_occurred; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_reading_meter_occurred ON public.meter_reading USING btree (meter, occurred);


--
-- Name: meter_service; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX meter_service ON public.meter USING btree (service);


--
-- Name: model_statistic_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX model_statistic_meter ON public.model_statistic USING btree (meter);


--
-- Name: monthly_budget_forecast_meter_start_end; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX monthly_budget_forecast_meter_start_end ON public.monthly_budget_forecast USING btree (meter, start, "end");


--
-- Name: monthly_fact_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX monthly_fact_meter ON public.monthly_fact USING btree (meter);


--
-- Name: monthly_yoy_variance_meter_end_start; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX monthly_yoy_variance_meter_end_start ON public.monthly_yoy_variance USING btree (meter, "end", start);


--
-- Name: mv_baseline_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX mv_baseline_meter ON public.mv_baseline USING btree (meter);


--
-- Name: mv_baseline_meter_baseline_fit; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX mv_baseline_meter_baseline_fit ON public.mv_baseline USING btree (meter, baseline_fit);


--
-- Name: mv_drift_data_period; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX mv_drift_data_period ON public.mv_drift_data USING btree (period);


--
-- Name: mv_drift_period_meter_baseline_period_comparison_start; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX mv_drift_period_meter_baseline_period_comparison_start ON public.mv_drift_period USING btree (meter, baseline_period, comparison_start);


--
-- Name: pdp_analytics_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX pdp_analytics_meter ON public.pdp_analytics USING btree (meter);


--
-- Name: pdp_event_has_interval_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX pdp_event_has_interval_index ON public.pdp_event USING btree (event, has_interval);


--
-- Name: pdp_event_has_weather_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX pdp_event_has_weather_index ON public.pdp_event USING btree (event, has_weather);


--
-- Name: pdp_event_weather_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX pdp_event_weather_index ON public.pdp_event_weather USING btree (event, has_data);


--
-- Name: peak_billing_fact_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_billing_fact_meter ON public.peak_billing_fact USING btree (meter);


--
-- Name: peak_forecast_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_forecast_meter ON public.peak_forecast USING btree (meter);


--
-- Name: peak_forecast_meter_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_forecast_meter_date ON public.peak_forecast USING btree (meter, date);


--
-- Name: peak_forecast_meter_date_holiday; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_forecast_meter_date_holiday ON public.peak_forecast USING btree (meter, date, holiday);


--
-- Name: peak_forecast_meter_date_tou; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_forecast_meter_date_tou ON public.peak_forecast USING btree (meter, date, tou);


--
-- Name: peak_history_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_history_meter ON public.peak_history USING btree (meter);


--
-- Name: peak_history_meter_date_holiday; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_history_meter_date_holiday ON public.peak_history USING btree (meter, date, holiday);


--
-- Name: peak_history_meter_date_tou; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_history_meter_date_tou ON public.peak_history USING btree (meter, date, tou);


--
-- Name: peak_history_outliers; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_history_outliers ON public.peak_history USING btree (meter, incomplete, date, actual_peak, predicted_peak);


--
-- Name: peak_prediction_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_prediction_meter ON public.peak_prediction USING btree (meter);


--
-- Name: peak_prediction_meter_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX peak_prediction_meter_date ON public.peak_prediction USING btree (meter, occurred);


--
-- Name: plotting_fact_meter_fact; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX plotting_fact_meter_fact ON public.plotting_fact USING btree (meter, fact);


--
-- Name: product_enrollment_meter_product; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX product_enrollment_meter_product ON public.product_enrollment USING btree (meter, product);


--
-- Name: rate_analysis__meter_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX rate_analysis__meter_index ON public.rate_analysis USING btree (meter);


--
-- Name: rate_analysis_meter_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX rate_analysis_meter_index ON public.rate_analysis USING btree (meter);


--
-- Name: rate_model_coefficient_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX rate_model_coefficient_meter ON public.rate_model_coefficient USING btree (meter);


--
-- Name: rate_right_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX rate_right_meter ON public.rate_right USING btree (meter);


--
-- Name: rate_right_summary_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX rate_right_summary_meter ON public.rate_right_summary USING btree (meter);


--
-- Name: rcx_pattern_analytics_meter_period; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX rcx_pattern_analytics_meter_period ON public.rcx_pattern_analytics USING btree (meter, period);


--
-- Name: real_time_attribute_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX real_time_attribute_meter ON public.real_time_attribute USING btree (meter);


--
-- Name: recommendation_indices_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX recommendation_indices_meter ON public.recommendation_indices USING btree (meter);


--
-- Name: report_account; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX report_account ON public.report USING btree (account);


--
-- Name: report_user; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX report_user ON public.report USING btree ("user");


--
-- Name: savings_estimates_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX savings_estimates_meter ON public.savings_estimates USING btree (meter);


--
-- Name: score_pattern_analytics_meter_period; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX score_pattern_analytics_meter_period ON public.score_pattern_analytics USING btree (meter, period);


--
-- Name: service_audit_idx; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX service_audit_idx ON public.bill_old USING btree (service, audit_complete, audit_successful, audit_suppressed, audit_accepted) WHERE ((audit_complete = true) AND (closing >= '2016-01-01'::date));


--
-- Name: snapmeter_bill_oid_idx; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX snapmeter_bill_oid_idx ON public.snapmeter_bill_view USING btree (oid);


--
-- Name: snapmeter_bill_service_closing_initial_idx; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX snapmeter_bill_service_closing_initial_idx ON public.snapmeter_bill_view USING btree (service, closing, initial, cost, used);


--
-- Name: standard_holiday_year_day; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX standard_holiday_year_day ON public.standard_holiday USING btree (year, day);


--
-- Name: stasis_transaction_target; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX stasis_transaction_target ON public.stasis_transaction USING btree (target);


--
-- Name: subscription_unique; Type: INDEX; Schema: public; Owner: gridium
--

CREATE UNIQUE INDEX subscription_unique ON public.snapmeter_user_subscription USING btree ("user", subscription, meter);


--
-- Name: temperature_response_meter_period_type; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX temperature_response_meter_period_type ON public.temperature_response USING btree (meter, period_type);


--
-- Name: test_stored_num; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX test_stored_num ON public.test_stored USING btree (num);


--
-- Name: trailing_twelve_month_analytics_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX trailing_twelve_month_analytics_meter ON public.trailing_twelve_month_analytics USING btree (meter);


--
-- Name: ttm_calculation_period; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX ttm_calculation_period ON public.ttm_calculation USING btree (period);


--
-- Name: ttm_fact_period; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX ttm_fact_period ON public.ttm_fact USING btree (period);


--
-- Name: ttm_period_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX ttm_period_meter ON public.ttm_period USING btree (meter);


--
-- Name: usage_history_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX usage_history_meter ON public.usage_history USING btree (meter);


--
-- Name: usage_history_outliers; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX usage_history_outliers ON public.usage_history USING btree (meter, incomplete, occurred, actual_use, predicted_use);


--
-- Name: use_prediction_meter; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX use_prediction_meter ON public.use_prediction USING btree (meter);


--
-- Name: use_prediction_meter_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX use_prediction_meter_date ON public.use_prediction USING btree (meter, occurred);


--
-- Name: utility_service_service_id; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX utility_service_service_id ON public.utility_service USING btree (service_id);


--
-- Name: variance_clause_analysis; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX variance_clause_analysis ON public.variance_clause USING btree (analysis);


--
-- Name: variance_clause_baseline; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX variance_clause_baseline ON public.variance_clause USING btree (baseline);


--
-- Name: weather_forecast_location_occurrence; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_forecast_location_occurrence ON public.weather_forecast USING btree (location, occurrence);


--
-- Name: weather_history_log_wds_date; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_history_log_wds_date ON public.weather_history_log USING btree (wds, date);


--
-- Name: weather_history_station_occurrence; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_history_station_occurrence ON public.weather_history USING btree (source, occurrence);


--
-- Name: weather_source_axis_coordinate_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_source_axis_coordinate_index ON public.weather_source_axis USING gist (coordinates);


--
-- Name: weather_station_code; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_station_code ON public.weather_station USING btree (code);


--
-- Name: weather_station_coordinate_index; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_station_coordinate_index ON public.weather_station USING gist (coordinates);


--
-- Name: weather_station_wban; Type: INDEX; Schema: public; Owner: gridium
--

CREATE INDEX weather_station_wban ON public.weather_station USING btree (wban);


--
-- Name: smd_authorization_audit_point audit_point_audit_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit_point
    ADD CONSTRAINT audit_point_audit_fkey FOREIGN KEY (audit) REFERENCES public.smd_authorization_audit(oid) ON DELETE CASCADE;


--
-- Name: auth_session auth_session_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.auth_session
    ADD CONSTRAINT auth_session_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.auth_user(id);


--
-- Name: smd_authorization_audit credential_audit_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit
    ADD CONSTRAINT credential_audit_fkey FOREIGN KEY (credential) REFERENCES public.pge_credential(oid) ON DELETE CASCADE;


--
-- Name: curtailment_peak curtailment_peak_recommendation_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.curtailment_peak
    ADD CONSTRAINT curtailment_peak_recommendation_fkey FOREIGN KEY (recommendation) REFERENCES public.curtailment_recommendation(oid) ON DELETE CASCADE;


--
-- Name: decomp_facts decomp_facts_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.decomp_facts
    ADD CONSTRAINT decomp_facts_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: green_button_notification green_button_notification_provider_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification
    ADD CONSTRAINT green_button_notification_provider_oid_fkey FOREIGN KEY (provider_oid) REFERENCES public.green_button_provider(oid);


--
-- Name: green_button_notification_resource green_button_notification_resource_notification_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_resource
    ADD CONSTRAINT green_button_notification_resource_notification_oid_fkey FOREIGN KEY (notification_oid) REFERENCES public.green_button_notification(oid);


--
-- Name: green_button_notification_task green_button_notification_task_owner_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_task
    ADD CONSTRAINT green_button_notification_task_owner_oid_fkey FOREIGN KEY (owner_oid) REFERENCES public.green_button_notification_resource(oid);


--
-- Name: green_button_notification_task green_button_notification_task_task_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_notification_task
    ADD CONSTRAINT green_button_notification_task_task_oid_fkey FOREIGN KEY (task_oid) REFERENCES public.green_button_task(oid);


--
-- Name: green_button_reading_stats green_button_reading_stats_reading_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_reading_stats
    ADD CONSTRAINT green_button_reading_stats_reading_fkey FOREIGN KEY (reading) REFERENCES public.green_button_meter_reading(oid);


--
-- Name: green_button_subscription_task green_button_subscription_task_task_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_subscription_task
    ADD CONSTRAINT green_button_subscription_task_task_oid_fkey FOREIGN KEY (task_oid) REFERENCES public.green_button_task(oid);


--
-- Name: green_button_task green_button_task_provider_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.green_button_task
    ADD CONSTRAINT green_button_task_provider_oid_fkey FOREIGN KEY (provider_oid) REFERENCES public.green_button_provider(oid);


--
-- Name: interval_facts interval_facts_daily_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.interval_facts
    ADD CONSTRAINT interval_facts_daily_fkey FOREIGN KEY (daily) REFERENCES public.daily_fact(oid) ON DELETE CASCADE;


--
-- Name: messaging_email_event messaging_email_events_email_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_email_event
    ADD CONSTRAINT messaging_email_events_email_id_fkey FOREIGN KEY (email_id) REFERENCES public.messaging_email(id);


--
-- Name: messaging_incoming_email messaging_incoming_email_reply_target_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_incoming_email
    ADD CONSTRAINT messaging_incoming_email_reply_target_id_fkey FOREIGN KEY (reply_target_id) REFERENCES public.messaging_email(id);


--
-- Name: messaging_sms_message_event messaging_sms_message_event_message_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.messaging_sms_message_event
    ADD CONSTRAINT messaging_sms_message_event_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messaging_sms_message(id);


--
-- Name: meter_bulk_edit_log meter_bulk_edit_log_editor_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_bulk_edit_log
    ADD CONSTRAINT meter_bulk_edit_log_editor_fkey FOREIGN KEY (editor) REFERENCES public.snapmeter_user(oid);


--
-- Name: meter_group_item meter_group_item_group_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_group_item
    ADD CONSTRAINT meter_group_item_group_fkey FOREIGN KEY ("group") REFERENCES public.meter_group(oid) ON DELETE CASCADE;


--
-- Name: meter_group_item meter_group_item_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.meter_group_item
    ADD CONSTRAINT meter_group_item_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: mv_assessment_timeseries mv_assessment_timeseries_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_assessment_timeseries
    ADD CONSTRAINT mv_assessment_timeseries_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: mv_assessment_timeseries mv_assessment_timeseries_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_assessment_timeseries
    ADD CONSTRAINT mv_assessment_timeseries_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program_cross_type(oid) ON DELETE CASCADE;


--
-- Name: mv_baseline_timeseries mv_baseline_timeseries_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_baseline_timeseries
    ADD CONSTRAINT mv_baseline_timeseries_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: mv_baseline_timeseries mv_baseline_timeseries_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_baseline_timeseries
    ADD CONSTRAINT mv_baseline_timeseries_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program_cross_type(oid) ON DELETE CASCADE;


--
-- Name: mv_drift_data mv_drift_data_period_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_drift_data
    ADD CONSTRAINT mv_drift_data_period_fkey FOREIGN KEY (period) REFERENCES public.mv_drift_period(oid) ON DELETE CASCADE;


--
-- Name: mv_exogenous_factor_timeseries mv_exogenous_factor_timeseries_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_exogenous_factor_timeseries
    ADD CONSTRAINT mv_exogenous_factor_timeseries_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: mv_exogenous_factor_timeseries mv_exogenous_factor_timeseries_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_exogenous_factor_timeseries
    ADD CONSTRAINT mv_exogenous_factor_timeseries_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program_cross_type(oid) ON DELETE CASCADE;


--
-- Name: mv_model_fit_statistic mv_model_fit_statistic_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_model_fit_statistic
    ADD CONSTRAINT mv_model_fit_statistic_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: mv_model_fit_statistic mv_model_fit_statistic_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_model_fit_statistic
    ADD CONSTRAINT mv_model_fit_statistic_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program_cross_type(oid) ON DELETE CASCADE;


--
-- Name: mv_nonroutine_event mv_nonroutine_event_meter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_nonroutine_event
    ADD CONSTRAINT mv_nonroutine_event_meter_fkey FOREIGN KEY (meter) REFERENCES public.meter(oid) ON DELETE CASCADE;


--
-- Name: mv_nonroutine_event mv_nonroutine_event_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_nonroutine_event
    ADD CONSTRAINT mv_nonroutine_event_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program_cross_type(oid) ON DELETE CASCADE;


--
-- Name: mv_program_cross_type mv_program_cross_type_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program_cross_type
    ADD CONSTRAINT mv_program_cross_type_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program(oid) ON DELETE CASCADE;


--
-- Name: mv_program_cross_type mv_program_cross_type_program_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_program_cross_type
    ADD CONSTRAINT mv_program_cross_type_program_type_fkey FOREIGN KEY (program_type) REFERENCES public.mv_program_type(oid) ON DELETE CASCADE;


--
-- Name: mv_project mv_project_building_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project
    ADD CONSTRAINT mv_project_building_fkey FOREIGN KEY (building) REFERENCES public.building(oid) ON DELETE CASCADE;


--
-- Name: mv_project mv_project_customer_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project
    ADD CONSTRAINT mv_project_customer_fkey FOREIGN KEY (customer) REFERENCES public.snapmeter_account(oid) ON DELETE CASCADE;


--
-- Name: mv_project mv_project_meter_group_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project
    ADD CONSTRAINT mv_project_meter_group_fkey FOREIGN KEY (meter_group) REFERENCES public.meter_group(oid) ON DELETE CASCADE;


--
-- Name: mv_project mv_project_program_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project
    ADD CONSTRAINT mv_project_program_fkey FOREIGN KEY (program) REFERENCES public.mv_program(oid) ON DELETE CASCADE;


--
-- Name: mv_project mv_project_weather_station_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.mv_project
    ADD CONSTRAINT mv_project_weather_station_fkey FOREIGN KEY (weather_station) REFERENCES public.weather_station(oid) ON DELETE CASCADE;


--
-- Name: real_time_attribute real_time_attribute_token_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.real_time_attribute
    ADD CONSTRAINT real_time_attribute_token_fkey FOREIGN KEY (token) REFERENCES public.access_token(oid);


--
-- Name: sce_gb_customer_account sce_gb_customer_account_retail_customer_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_customer_account
    ADD CONSTRAINT sce_gb_customer_account_retail_customer_oid_fkey FOREIGN KEY (retail_customer_oid) REFERENCES public.sce_gb_retail_customer(oid) ON DELETE CASCADE;


--
-- Name: sce_gb_customer_agreement sce_gb_customer_agreement_account_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.sce_gb_customer_agreement
    ADD CONSTRAINT sce_gb_customer_agreement_account_oid_fkey FOREIGN KEY (account_oid) REFERENCES public.sce_gb_customer_account(oid) ON DELETE CASCADE;


--
-- Name: smd_authorization_audit_point smd_authorization_audit_point_audit_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.smd_authorization_audit_point
    ADD CONSTRAINT smd_authorization_audit_point_audit_fkey FOREIGN KEY (audit) REFERENCES public.smd_authorization_audit(oid);


--
-- Name: snapmeter_account_data_source snapmeter_account_data_source_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_data_source
    ADD CONSTRAINT snapmeter_account_data_source_account_fkey FOREIGN KEY (account) REFERENCES public.snapmeter_account(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_account_meter snapmeter_account_meter_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_meter
    ADD CONSTRAINT snapmeter_account_meter_account_fkey FOREIGN KEY (account) REFERENCES public.snapmeter_account(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_account_user snapmeter_account_user_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_user
    ADD CONSTRAINT snapmeter_account_user_account_fkey FOREIGN KEY (account) REFERENCES public.snapmeter_account(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_account_user snapmeter_account_user_user_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_account_user
    ADD CONSTRAINT snapmeter_account_user_user_fkey FOREIGN KEY ("user") REFERENCES public.snapmeter_user(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_building snapmeter_building_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_building
    ADD CONSTRAINT snapmeter_building_account_fkey FOREIGN KEY (account) REFERENCES public.snapmeter_account(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_building snapmeter_building_building_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_building
    ADD CONSTRAINT snapmeter_building_building_fkey FOREIGN KEY (building) REFERENCES public.building(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_data_gap snapmeter_data_gap_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_data_gap
    ADD CONSTRAINT snapmeter_data_gap_account_fkey FOREIGN KEY (account) REFERENCES public.snapmeter_account(oid);


--
-- Name: snapmeter_meter_data_source snapmeter_meter_data_source_account_data_source_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_meter_data_source
    ADD CONSTRAINT snapmeter_meter_data_source_account_data_source_fkey FOREIGN KEY (account_data_source) REFERENCES public.snapmeter_account_data_source(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_provisioning snapmeter_provisioning_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning
    ADD CONSTRAINT snapmeter_provisioning_account_fkey FOREIGN KEY (account) REFERENCES public.snapmeter_account(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_provisioning_event snapmeter_provisioning_event_workflow_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_event
    ADD CONSTRAINT snapmeter_provisioning_event_workflow_fkey FOREIGN KEY (workflow) REFERENCES public.snapmeter_provisioning_workflow(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_provisioning_workflow snapmeter_provisioning_workflow_credential_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_workflow
    ADD CONSTRAINT snapmeter_provisioning_workflow_credential_fkey FOREIGN KEY (credential) REFERENCES public.snapmeter_provisioning_credential(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_provisioning_workflow snapmeter_provisioning_workflow_parent_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_provisioning_workflow
    ADD CONSTRAINT snapmeter_provisioning_workflow_parent_fkey FOREIGN KEY (parent) REFERENCES public.snapmeter_provisioning(oid) ON DELETE CASCADE;


--
-- Name: snapmeter_user_subscription snapmeter_user_subscription_user_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.snapmeter_user_subscription
    ADD CONSTRAINT snapmeter_user_subscription_user_fkey FOREIGN KEY ("user") REFERENCES public.snapmeter_user(oid) ON DELETE CASCADE;


--
-- Name: ttm_calculation ttm_calculation_period_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_calculation
    ADD CONSTRAINT ttm_calculation_period_fkey FOREIGN KEY (period) REFERENCES public.ttm_period(oid) ON DELETE CASCADE;


--
-- Name: ttm_fact ttm_fact_period_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.ttm_fact
    ADD CONSTRAINT ttm_fact_period_fkey FOREIGN KEY (period) REFERENCES public.ttm_period(oid) ON DELETE CASCADE;


--
-- Name: utility_tariff utility_tariff_utility_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.utility_tariff
    ADD CONSTRAINT utility_tariff_utility_fkey FOREIGN KEY (utility) REFERENCES public.utility(identifier) ON DELETE CASCADE;


--
-- Name: variance_clause variance_clause_analysis_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.variance_clause
    ADD CONSTRAINT variance_clause_analysis_fkey FOREIGN KEY (analysis) REFERENCES public.budget_aggregation(oid) ON DELETE CASCADE;


--
-- Name: variance_clause variance_clause_baseline_fkey; Type: FK CONSTRAINT; Schema: public; Owner: gridium
--

ALTER TABLE ONLY public.variance_clause
    ADD CONSTRAINT variance_clause_baseline_fkey FOREIGN KEY (baseline) REFERENCES public.budget_aggregation(oid) ON DELETE CASCADE;






