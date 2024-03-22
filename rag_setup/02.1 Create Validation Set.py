# Databricks notebook source
# MAGIC %run ../_resources/00-init-advanced $reset_all_data=false

# COMMAND ----------

# Generate volume_folder path
volume_folder = f"/Volumes/{catalog}/{db}/volume_rr_documentation/evaluation_dataset"

# COMMAND ----------

eval_set = [["What was the engine model and engine manufacturer involved in the incident?",
"""Pratt &amp; Whitney PW4077 turbo fan engine. The right engine suffered a fan blade out event. The
right engine was manufactured in 1995 and installed on the accident airplane in August 2016. At the
time of the event, the engine had accumulated 12,384 hours and 2,979 cycles since overhaul and
81,768 hours and 15,262 cycles since new"""],
["Describe the aircraft involved in the incident?",
"""Boeing 777-222 twin-engine, transport category airplane. Its registration is N772UA and serial
number 26930. It was manufactured in 1995, has 400 seats with a certified Max Gross weight of
545000 lbs. The date of the last inspection was January 16 th 2021 and the airframe total time at that
inspection was 9,6814.41 Hrs."""],
["Where was the debris field and over what area did it extend?",
"""Broomfield, Colorado; multiple pieces of the
engine inlet, fan cowls, and thrust reversers separated from the airplane and were found
scattered over an area of about 40 acres, including a public park and residential areas. There
were no ground injuries reported."""],
["What is a Pratt &amp; Whitney PW4077 turbo fan engine?",
"""The PW4077 is a dual-spool, axial flow, high-bypass turbofan engine that features a singlestage,
112-inch diameter fan (low pressure compressor [LPC] 1st stage), a 6-stage LPC, 11- stage high
pressure compressor (HPC), annular combustor, a 2-stage high pressure turbine (HPT) that drives the
HPC, and a 7-stage low pressure turbine (LPT) that drives the fan and LPC. Each engine is attached to
a pylon on its respective wing. The engine inlet is attached to the forward end of the engine, the fan
cowls are attached around the center portion of the engine, and the thrust reversers are attached
around the aft portion of the engine"""],
["What Actions were taken to ensure safety after this incient?",
"""Following this event, the FAA issued Emergency AD 2021-05-51, effectively grounding all
PW4000 112”-powered 777-200 and -300 airplanes so that a one-time TAI inspection of the 1 st stage
LPC blades could be completed. On October 21, 2021, P&amp;W issued Alert Service Bulletin (SB) PW4G-
112-A72-361, Engine – Blade Assembly, 1st Stage, Low Pressure Compressor (LPC) – Ultrasonic
Testing (UT) Inspection and Thermal Acoustic Image (TAI) Inspection of 1st Stage LPC Blade
Assemblies to Find Airfoil Cracks. The SB included both immediate and repetitive UT inspections for
three specific high-risk areas on the PW4000 112-inch hollow fan blades. It also added a required TAI
inspection every 1,000 cycles for all 1st stage LPC blades. The inspections included in this SB were
subsequently made mandatory on April 15, 2022, when the FAA issued Airworthiness Directive (AD)
2022-06-09.
Also in response to this event, Boeing developed an interim design solution incorporating engine
nacelle modifications, and Boeing subsequently issued multiple alert SBs for fan cowl inspections;
modifications to the inlet cowls and thrust reversers on 777-200 and -300 airplanes equipped with
PW4000 series engines; and inspection/repair of fan cowls for moisture ingression. These SBs were
subsequently mandated via FAA ADs 2022-06-10 and 2022-06-11."""],
["Who were the Pilots?",
"""The captain, age 60, was located in the left seat, held an airline transport pilot certificate with a
rating for airplane multiengine land and multiple type ratings, including the B-777. His most recent
first-class Federal Aviation Administration (FAA) medical certificate was issued on February 23, 2021.
Operator records indicated that the captain had 28,062 total hours of flight experience, including
414 hours on the B-777 in the previous 12 months. His most recent proficiency check was completed
on February 5, 2021. He held a Class 1 medical Certificate without waivers/limitations.
The first officer, age 54, occupied the right seat and held an airline transport pilot certificate with a
rating for airplane multiengine land and multiple type ratings, including the B-777. Operator records
indicated that the first officer had 18,612 total hours of flight experience, including 355 hours on the
B-777 in the previous 12 months. His most recent proficiency check was completed on November 27,
2020. He held a Class 1 medical Certificate with waivers/limitations."""],
["Summarise the incident in a paragraph?",
"""United Airlines flight 328 was climbing through 12,500 ft mean sea level about 5 minutes after
departure from Denver International Airport (DEN), Denver, Colorado, when the right engine, a Pratt
&amp; Whitney PW4077, sustained a full-length fan blade separation, or fan blade out (FBO) event. This
resulted in the subsequent separation of the engine inlet lip skin, fan cowl support beam, and
components of the inlet, fan cowls, and thrust reversers (TRs), as well as an engine fire. The flight
crew declared an emergency and landed the airplane without incident at the departure airport
about 24 minutes after takeoff. There were no injuries to the passengers or crew, and no ground
injuries.
Q8: Have there been other Fan Blade Out incidents on this engine type?
R8: This event was the fourth in-service FBO event due to fatigue cracking recorded for PW4000-
powered 777 airplanes and resulted in the most nacelle damage of the four events. In the first event
in 2010, approximately 50 percent of the blade airfoil was released. Full-span separations occurred
in 2018, 2021, and during this event."""],
["What caused the fan blade to be released?",
"""fan blade was fractured transversely across the chord of the airfoil near the fan hub fairing as the
result of a fatigue crack, which originated at the surface of an internal radius in a hollow cavity
within the blade. The event blade had accumulated 2,979 cycles since overhaul; at the time of the
event. Examination of the crack that led to the failure of the blade revealed a discontinuity in a local
tight radius in the internal blade geometry that had been introduced during the machining and
manufacturing process. A P&amp;W technical review of the discontinuity estimated a local steady stress
increase of 30% at the location, reducing the fatigue life by 50%.
In addition to the primary fracture, multiple fatigue cracks were identified in the flowpath and
midspan of the fractured blade. These secondary fatigue cracks had origins at the internal cavity
surfaces, and many of the cracks exhibited multiple origins, consistent with the primary fracture. The
largest of the secondary cracks had a maximum crack depth of 0.065 inch."""],

["Was there anything about the manufacturing process that contributed to this crack?",
"""R10: Metallurgical and chemical characterization revealed that the surfaces of the internal cavities
were contaminated with carbon that had diffused into the parent material. According to P&amp;W
engineers, carbon contamination of titanium can cause decreased fatigue resistance capability. It
was observed that the carbon surface contamination was not present in the cavities of the blade
that are sealed off during the diffusion bonding process, indicating that the contamination was
introduced after the diffusion bonding process.
Review of the manufacturing process revealed that the most likely source of the carbon
contamination was the shop argon system used during the hot final forming process. Before 1997,
P&amp;W’s high-pressure argon was supplied through the regular shop lines, which are not cleaned and
can contain various contaminants. In 1997, P&amp;W began using a clean dedicated argon system. The
hot final forming of the event blade occurred in 1994."""]]

# COMMAND ----------

import pandas as pd
df = spark.createDataFrame(pd.DataFrame(eval_set,columns = ['question','answer']))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save(volume_folder)

# COMMAND ----------


