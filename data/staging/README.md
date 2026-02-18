Staging Templates (Subcategory-wise)

Use these files to stage new payout rows by UI vehicle subcategory.
After confirmation, rows can be published to DB.

Rules:
- Blank cell = applicable to all (except Business_Type blank = Old/Renewal/Rollover).
- Policy type normalization while entering:
  - TP -> SATP
  - OD -> SAOD
  - Package / 1+1 -> Comprehensive(1+1)
  - 1+3 -> Bundle(1+3)
  - 1+5 -> Bundle(1+5)
  - 5+5 -> Bundle(5+5)
  - Package, TP -> Comprehensive(1+1),SATP
- Keep % style payout values (example: 33.81).

Templates created:
- two_wheeler_bike.xlsx
- two_wheeler_scooter.xlsx
- private_car.xlsx
- pcv_auto.xlsx
- pcv_taxi.xlsx
- pcv_staff_bus.xlsx
- pcv_educational_bus.xlsx
- pcv_edu_bus_under_school_name.xlsx
- pcv_other_subcategories.xlsx
- misc.xlsx
- gcv_3_wheeler_goods.xlsx
- gcv_flatbed.xlsx
- gcv_4_wheeler_goods.xlsx
