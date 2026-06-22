# เขียน DAG ให้ trace-weaver อ่าน lineage ได้เอง

> คู่มือสำหรับ data engineer: เขียนโค้ด pandas/Spark ใน DAG อย่างไรให้ `trace-weaver`
> ดึง **column-level lineage** ออกมาได้อัตโนมัติ โดย **ไม่ต้องเขียน `column_map` เอง**.
> trace-weaver อ่านโค้ดแบบ static (ไม่รัน DAG) — มันจึงอ่านได้เฉพาะสิ่งที่ "เห็นชื่อ column
> และที่มาได้ตรง ๆ จากโค้ด". เขียนตามคู่มือนี้แล้ว lineage จะออกมาครบเองโดยแก้โค้ดน้อยที่สุด.

## กฎเดียวที่ต้องจำ

> **ชื่อ column เป็น literal (เขียนตรง ๆ ในโค้ด) + ที่มาอ้าง column ตรง ๆ → เครื่องอ่านเอง**
>
> ถ้าชื่อ column มาจาก *ค่าตอนรัน* (ตัวแปร, ข้อมูล, config) หรือ logic ซ่อนใน *ฟังก์ชัน/lambda* →
> เครื่องอ่านไม่ได้ จะขึ้น `W_OPAQUE_COLUMN` ชี้บรรทัดให้ → ต้อง refactor หรือ declare

---

## ✅ เขียนแบบนี้ — อ่านเองได้ (ไม่ต้อง `column_map`)

### pandas
```python
bronze = pd.read_sql("SELECT * FROM bronze_sales", con=ENGINE)   # ← input = bronze_sales
silver = pd.DataFrame()
silver["event_id"]   = bronze["event_id"]                # identity:  event_id  <- event_id
silver["amount_usd"] = bronze["amount"] * 1.08           # transform: amount_usd <- amount
silver["total"]      = bronze["a"] + bronze["b"]         # fan-in:    total <- a, b
silver["full"]       = bronze.apply(lambda r: r["first"] + r["last"], axis=1)  # inline lambda over columns — อ่านได้
silver = silver.rename(columns={"amount": "amt"})        # rename:    amt <- amount
silver = bronze.assign(net=bronze["amount"] - bronze["fee"])
keep   = bronze[["event_id", "amount"]]                  # subset:    passthrough
silver.to_sql("silver_sales", con=ENGINE)                # ← output = silver_sales

# aggregate
g = bronze.groupby("region").agg({"amount": "sum", "qty": "max"})   # region (key) + amount,qty (agg)
g = bronze.groupby("region").agg(total=("amount", "sum"))          # named-agg: total <- amount

# loop ที่ใช้ list literal — unroll ให้อัตโนมัติ
for c in ["event_id", "region", "amount"]:
    silver[c] = bronze[c].fillna(0)
```

### PySpark
```python
bronze = spark.read.table("bronze_sales")            # หรือ spark.sql("SELECT ... FROM t")
silver = (bronze
    .withColumn("amount_usd", col("amount") * 1.08)   # transform
    .withColumn("amount_thb", expr("amount_usd * 36")) # expr("…") = SQL string, อ่านผ่าน SQL parser
    .withColumnRenamed("id", "event_id")              # rename
    .select("event_id", col("amount").alias("amt"))   # select + alias
    .selectExpr("event_id", "amount * 1.08 AS usd"))   # selectExpr ก็อ่านได้ (ต้องมี AS)
silver.write.saveAsTable("silver_sales")             # ← output

# aggregate — ต้องมี .alias() เพื่อตั้งชื่อ
gold = bronze.groupBy("region").agg(F.sum(col("amount")).alias("total"))
gold.write.saveAsTable("gold_sales")
```

**สรุปสิ่งที่อ่านได้:** `read_sql`/`read_sql_table`/`spark.read.table`/`spark.sql` (→ input) ·
`to_sql`/`saveAsTable` (→ output) · `df["c"]=expr`, `withColumn`, `withColumnRenamed`,
`select`/`selectExpr`, `rename(columns={...})`, `assign(...)`, `df[[...]]`, `groupby/agg`,
`expr("...")`, และ loop เหนือ **list literal**.

---

## ❌ อย่าเขียนแบบนี้ — เครื่องอ่านไม่ได้ (จะขึ้น `W_OPAQUE_COLUMN`)

| ห้าม | ทำไมอ่านไม่ได้ |
|---|---|
| `out[col_var] = ...` (ชื่อ column มาจากตัวแปร) | ชื่อ column ไม่ใช่ literal — รู้ตอนรันเท่านั้น |
| `out["c"] = df.apply(named_func, axis=1)` / `F.udf(...)` / `.rdd.map(...)` | logic อยู่ใน**ฟังก์ชันมีชื่อ/ภายนอก** เครื่องไม่เข้าไปอ่าน *(แต่ inline `lambda r: r["a"]+r["b"]` อ่านได้แล้ว)* |
| `out = a.merge(b, on="k")` แล้วใช้ column ที่ไม่ใช่ key | ต้องรู้ schema ของทั้ง 2 ฝั่ง (ซึ่งไม่มี) |
| `df.pivot(...)` / `melt` / `explode` / `stack` / `unstack` | ชื่อ column ผลลัพธ์ = **ค่าข้อมูล** ตอนรัน |
| `df.columns = [...]` (เปลี่ยนชื่อยกแผง) | อิงลำดับ column ตอนรัน |
| `rename(columns=runtime_dict)` (dict ไม่ใช่ literal) | mapping รู้ตอนรัน |
| `.agg(F.sum("a"))` แบบไม่มี `.alias()` (Spark) | Spark ตั้งชื่อ `sum(a)` เองตอนรัน |
| `for c in get_cols():` (list ไม่ใช่ literal) | รายชื่อ column รู้ตอนรัน |

---

## 🔧 เจอ case ไหน → เขียนใหม่แบบนี้ (แก้น้อยสุด)

| ❌ แบบเดิม (opaque) | ✅ เขียนใหม่ (อ่านได้, ผลเหมือนเดิม) |
|---|---|
| `out["c"] = df.apply(my_udf, axis=1)` *(ฟังก์ชันมีชื่อ)* | inline ลง: `out["c"] = df.apply(lambda r: r["a"] + r["b"], axis=1)` หรือ vectorize `df["a"] + df["b"]` *(เร็วกว่า)* |
| `col = "amount_usd"; out[col] = df["amount"] * 1.08` | `out["amount_usd"] = df["amount"] * 1.08` *(ใช้ชื่อ literal ตรง ๆ)* |
| `for c in cols: out[c] = df[c]` *(cols เป็นตัวแปร)* | `for c in ["a", "b", "c"]: out[c] = df[c]` *(เขียน list ตรง ๆ)* |
| `df.selectExpr("amount * 1.08")` *(ไม่มี AS)* | `df.selectExpr("amount * 1.08 AS amount_usd")` *(ใส่ `AS ชื่อ`)* |
| `gold.agg(F.sum(col("amount")))` *(ไม่มีชื่อ)* | `gold.agg(F.sum(col("amount")).alias("total"))` *(ใส่ `.alias`)* |
| `s = clean(df)` *(logic อยู่ในฟังก์ชัน)* | ย้ายโค้ดแปลง column มาเขียนใน task body ตรง ๆ |
| `out = a.merge(b, on="cust_id")` แล้วใช้ `b` columns | merge ได้ แต่ **column ที่มาจาก `b` ให้ declare** (ดูข้างล่าง) |
| `df.pivot(columns="region")` | ถ้าทราบค่าก็แตกเป็น `withColumn` ราย column; ถ้าไม่ → declare |

> หลักการ refactor: **ตัด indirection ออก** (เลิก lambda/UDF/ตัวแปรชื่อ column) แล้วเขียน
> column ตรง ๆ จาก column ต้นทาง — โค้ดได้ผลเท่าเดิม แต่เครื่องอ่าน lineage ได้

---

## ถ้า refactor ไม่ได้จริง ๆ → declare เฉพาะ column นั้น

`W_OPAQUE_COLUMN` บอกชื่อ task + บรรทัดให้แล้ว — ใส่ `column_map` ให้ **เฉพาะ column ที่ถูก flag**
(declared ชนะ inference เสมอ และเคลียร์ warning ทันที). ไม่ต้องประกาศทั้ง task:

```python
@tw.task(
    inputs=["bronze_sales", "customers"],
    outputs=["enriched"],
    engine="pandas",
    # ประกาศเฉพาะ column ที่เครื่องตามไม่ได้:
    column_map=[
        (["customers.name"], "cust_name", "join from customers"),     # join non-key
        (["amount", "fee"], "score", "row-wise UDF: amount - fee"),    # UDF
    ],
)
def build_enriched():
    ...
```

- `copy=["a", "b"]` = ทางลัดสำหรับ column ที่ **ส่งผ่านชื่อเดิม** (identity) — สั้นกว่า `column_map`
- column ที่เครื่องอ่านได้อยู่แล้ว **ไม่ต้องใส่** — ปล่อยให้ auto

---

## Checklist สั้น ๆ ก่อน merge

1. ชื่อ column ทุกตัวที่สร้าง เป็น **string literal** ใช่ไหม (ไม่ใช่ตัวแปร/loop ที่ไม่ใช่ literal)
2. ไม่มี UDF/ฟังก์ชัน**มีชื่อ**ที่ห่อ logic ของ column (`.apply(named_func)`, `.rdd`) — inline `lambda r: r["a"]+r["b"]` อ่านได้ แต่ vectorize เร็วกว่า
3. Spark: aggregate ทุกตัวมี `.alias("ชื่อ")`; `selectExpr`/`expr` ที่ derive มี `AS ชื่อ`
4. รัน `trace-weaver scan dags/` แล้ว **ไม่มี `W_OPAQUE_COLUMN`** (ถ้ามี → refactor ตามตาราง หรือ declare)
