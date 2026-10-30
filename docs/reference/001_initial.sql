CREATE SCHEMA kinda;

CREATE TABLE kinda.stream_sources (
    source_name text PRIMARY KEY,
    kafka_cluster_id text NOT NULL,
    topic text NOT NULL,
    kafka_topic_id uuid NOT NULL,
    stream_epoch uuid NOT NULL UNIQUE
);

CREATE TABLE kinda.branches (
    branch_id text PRIMARY KEY CHECK (branch_id IN ('BR001','BR002','BR003')),
    name text NOT NULL,
    timezone text NOT NULL DEFAULT 'Asia/Bangkok' CHECK (timezone = 'Asia/Bangkok')
);
INSERT INTO kinda.branches(branch_id,name) VALUES
    ('BR001','Kinda demo 1'),('BR002','Kinda demo 2'),('BR003','Kinda demo 3');

CREATE TABLE kinda.products (
    product_id text PRIMARY KEY,
    name text NOT NULL,
    category text NOT NULL CHECK (category IN ('bread','smoothie'))
);
INSERT INTO kinda.products VALUES
    ('BREAD_VANILLA','ขนมปังไส้ครีมวานิลลา','bread'),
    ('BREAD_CHOCOLATE','ขนมปังไส้ครีมช็อกโกแลต','bread'),
    ('SMOOTHIE_MANGO','น้ำมะม่วงปั่น','smoothie');

CREATE TABLE kinda.sale_events (
    event_id uuid PRIMARY KEY,
    source_system text NOT NULL CHECK (source_system IN ('simulator','wongnai_csv')),
    source_instance text NOT NULL,
    branch_id text NOT NULL REFERENCES kinda.branches,
    business_date date NOT NULL,
    receipt_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    currency text NOT NULL CHECK (currency = 'THB'),
    tax_mode text NOT NULL CHECK (tax_mode IN ('explicit_included_component','not_provided')),
    gross_amount numeric(18,2) NOT NULL CHECK (gross_amount BETWEEN 0 AND 9999999999.99),
    discount_amount numeric(18,2) NOT NULL CHECK (discount_amount BETWEEN 0 AND gross_amount),
    net_amount numeric(18,2) NOT NULL CHECK (net_amount = gross_amount - discount_amount),
    tax_amount numeric(18,2) NOT NULL CHECK (tax_amount BETWEEN 0 AND net_amount),
    semantic_hash text NOT NULL CHECK (semantic_hash ~ '^[0-9a-f]{64}$'),
    canonical_sale jsonb NOT NULL CHECK (jsonb_typeof(canonical_sale) = 'object'),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    UNIQUE (source_system,source_instance,branch_id,business_date,receipt_id),
    CHECK (tax_mode <> 'not_provided' OR tax_amount = 0),
    CHECK (business_date = (occurred_at AT TIME ZONE 'Asia/Bangkok')::date)
);

CREATE TABLE kinda.sale_lines (
    event_id uuid NOT NULL REFERENCES kinda.sale_events,
    line_id text NOT NULL,
    product_id text NOT NULL REFERENCES kinda.products,
    quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price numeric(18,2) NOT NULL CHECK (unit_price BETWEEN 0 AND 9999999999.99),
    gross_amount numeric(18,2) NOT NULL CHECK (gross_amount BETWEEN 0 AND 9999999999.99),
    discount_amount numeric(18,2) NOT NULL CHECK (discount_amount BETWEEN 0 AND gross_amount),
    net_amount numeric(18,2) NOT NULL CHECK (net_amount = gross_amount - discount_amount),
    tax_amount numeric(18,2) NOT NULL CHECK (tax_amount BETWEEN 0 AND net_amount),
    PRIMARY KEY (event_id,line_id),
    CHECK (gross_amount = quantity * unit_price)
);

CREATE TABLE kinda.deliveries (
    stream_epoch uuid NOT NULL,
    topic text NOT NULL,
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    offset_id bigint NOT NULL CHECK (offset_id >= 0),
    kafka_timestamp timestamptz,
    kafka_key bytea,
    raw_value bytea,
    outcome text NOT NULL CHECK (outcome IN ('accepted','duplicate','rejected')),
    canonical_event_id uuid REFERENCES kinda.sale_events,
    error_code text,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stream_epoch,topic,partition_id,offset_id),
    CHECK ((outcome = 'rejected' AND error_code IS NOT NULL AND canonical_event_id IS NULL)
        OR (outcome IN ('accepted','duplicate') AND error_code IS NULL AND canonical_event_id IS NOT NULL))
);
CREATE INDEX sale_events_branch_time ON kinda.sale_events(branch_id,occurred_at);
CREATE INDEX sale_events_date ON kinda.sale_events(business_date,branch_id);
CREATE INDEX sale_lines_product ON kinda.sale_lines(product_id,event_id);
CREATE INDEX deliveries_errors ON kinda.deliveries(processed_at) WHERE outcome = 'rejected';

CREATE FUNCTION kinda.guard_ledger_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE header_xid xid8;
BEGIN
    IF TG_TABLE_NAME = 'sale_events' THEN
        NEW.created_xid := pg_current_xact_id();
    ELSE
        SELECT created_xid INTO header_xid FROM kinda.sale_events WHERE event_id = NEW.event_id;
        IF header_xid IS DISTINCT FROM pg_current_xact_id() THEN
            RAISE EXCEPTION 'IMMUTABLE_RECEIPT %', NEW.event_id USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER sale_header_insert BEFORE INSERT ON kinda.sale_events
    FOR EACH ROW EXECUTE FUNCTION kinda.guard_ledger_insert();
CREATE TRIGGER sale_line_insert BEFORE INSERT ON kinda.sale_lines
    FOR EACH ROW EXECUTE FUNCTION kinda.guard_ledger_insert();

CREATE FUNCTION kinda.check_sale_totals() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    sale_id uuid;
    header kinda.sale_events%ROWTYPE;
    totals record;
BEGIN
    IF TG_OP = 'DELETE' THEN sale_id := OLD.event_id; ELSE sale_id := NEW.event_id; END IF;
    SELECT * INTO header FROM kinda.sale_events WHERE event_id = sale_id;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT count(*) AS n, sum(gross_amount) AS gross, sum(discount_amount) AS discount,
           sum(net_amount) AS net, sum(tax_amount) AS tax
      INTO totals FROM kinda.sale_lines WHERE event_id = sale_id;
    IF totals.n NOT BETWEEN 1 AND 100 OR totals.gross IS DISTINCT FROM header.gross_amount
       OR totals.discount IS DISTINCT FROM header.discount_amount
       OR totals.net IS DISTINCT FROM header.net_amount
       OR totals.tax IS DISTINCT FROM header.tax_amount THEN
        RAISE EXCEPTION 'SALE_TOTAL_MISMATCH %', sale_id USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER sale_header_totals AFTER INSERT OR UPDATE ON kinda.sale_events
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION kinda.check_sale_totals();
CREATE CONSTRAINT TRIGGER sale_line_totals AFTER INSERT OR UPDATE OR DELETE ON kinda.sale_lines
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION kinda.check_sale_totals();

CREATE VIEW kinda.v_branch_daily_sales AS
SELECT source_system,source_instance,branch_id,business_date,count(*) AS receipt_count,
       sum(gross_amount) AS gross_sales,sum(discount_amount) AS discounts,
       sum(net_amount) AS sales_after_discount_before_returns,
       sum(tax_amount) FILTER (WHERE tax_mode = 'explicit_included_component') AS known_included_tax,
       count(*) FILTER (WHERE tax_mode = 'not_provided') AS receipts_without_tax_detail
  FROM kinda.sale_events GROUP BY source_system,source_instance,branch_id,business_date;

CREATE VIEW kinda.v_branch_product_daily_sales AS
SELECT e.source_system,e.source_instance,e.branch_id,e.business_date,l.product_id,sum(l.quantity) AS units,
       sum(l.net_amount) AS sales_after_discount_before_returns
  FROM kinda.sale_events e JOIN kinda.sale_lines l USING(event_id)
 GROUP BY e.source_system,e.source_instance,e.branch_id,e.business_date,l.product_id;

CREATE VIEW kinda.v_rejections AS
SELECT * FROM kinda.deliveries WHERE outcome = 'rejected';
