# Site Consolidation Suitability Analysis: Domain Explainer &amp; Dataset Guide
## What Is This Problem About?
This challenge asks you to build a **data-driven method for identifying land parcels in Victoria that are good candidates for site consolidation** — that is, merging adjacent small lots into larger development sites. The Victorian Government's *Housing Statement* and *Plan for Victoria* set ambitious housing targets, and one key mechanism to meet those targets is enabling **medium-density housing (3–6 storeys)** in well-connected locations through the new **Housing Choice and Transport Zone (HCTZ)**. The HCTZ incentivises lot consolidation by allowing greater building height on larger consolidated sites.[^1]

The practical goal: using Vicmap parcel geometry and other public spatial data, derive features that describe each parcel's shape, size, edges, and planning context, then score which parcels (or clusters of parcels) have the highest potential for consolidation.[^2]

***
## Key Domain Concepts Explained
### Site Consolidation
Site consolidation means **combining two or more adjacent small land parcels into a single larger development site**. Developers do this because many existing suburban lots are too small or too narrow to efficiently build multi-storey housing. A consolidated site can achieve better floor-plate efficiency, meet minimum frontage requirements, and yield more dwellings under height controls.
### Housing Choice and Transport Zone (HCTZ)
The HCTZ is a new residential zone introduced in 2023 as part of Victoria's Housing Statement. It targets areas within **800 metres of a train or tram stop**, or within **400 metres of a high-frequency bus route**. The zone allows 3–6 storey development and explicitly encourages lot consolidation by tying building height to site size.[^1]
### Activity Centres (AC) and PT Nodes
Activity centres are hubs of retail, employment, services, and housing — they range from Metropolitan Activity Centres (like Box Hill, Sunshine) to Major and Neighbourhood Activity Centres. **PT nodes** are public transport stations/stops. Parcels near these nodes are prime targets for consolidation because planning policy directs growth toward these locations.[^3][^4]
### Planning Overlays
Overlays are additional planning controls that reflect specific land characteristics. Key overlays relevant as **constraints** to consolidation include:[^5]
- **Heritage Overlay (HO)** — restricts demolition/alteration of heritage buildings
- **Land Subject to Inundation Overlay (LSIO)** / **Special Building Overlay (SBO)** — flood-prone land
- **Environmental Significance Overlay (ESO)** — protects sensitive environments[^6]
- **Vegetation Protection Overlay (VPO)** — protects significant trees

***
## Phase 1: Deriving Topological &amp; Geometric Features
Phase 1 is about **engineering spatial features** from raw parcel geometry. These features become inputs for the scoring model in Phase 2.
### Parcel Geometric Attributes
These quantify the shape and size of each parcel:

| Feature | What It Measures | Why It Matters |
|---|---|---|
| **Lot area** | Total area of the parcel | Parcels below minimum site area thresholds need consolidation |
| **Perimeter** | Boundary length | Relates to setback exposure |
| **Aspect ratio** (width ÷ depth) | Proportionality of the lot | Narrow or deep lots are less efficient |
| **Min/max/avg lot width** | Width at various points | Must meet minimum frontage requirements |
| **Compactness index** | How close to a circle (4π × area ÷ perimeter²) | Irregular shapes waste buildable area |
| **Elongation index** | Ratio of length to width | Highly elongated lots are harder to develop |
### Parcel Edge Topology
This is about understanding **what each edge of a parcel touches** — another parcel, a road, or public land:

| Feature | What It Measures | Why It Matters |
|---|---|---|
| **Number of road frontages** | How many distinct road edges | Corner lots have two; internal lots may have one |
| **Frontage length per type** | Length along arterial vs local road | Affects access and planning requirements |
| **Shared parcel boundaries** | Length of edges shared with neighbours | More shared boundary = easier to consolidate |
| **Longest continuous frontage** | Single longest road-facing edge | Must meet minimum frontage threshold |
| **Corner lot indicator** | Boolean: two or more road frontages | Corner lots have different setback rules |
| **Laneway access** | Edge touching a laneway | Secondary access can improve development potential |
| **Frontage-to-perimeter ratio** | Proportion of boundary that is road | Low ratio = mostly enclosed by other parcels |

***
## Phase 2: Multi-Criteria Suitability Scoring
Phase 2 uses the features from Phase 1 to **score each parcel** on its consolidation suitability.
### Opportunities (factors that increase suitability)
- **Within HCTZ zone** or close to PT nodes / Activity Centres — policy supports higher density here
- **Below minimum site area** — the lot is too small to develop alone
- **Below minimum frontage** — the lot doesn't have enough road frontage for a multi-unit development
- **Irregular or inefficient shape** — the lot is hard to develop without combining with a neighbour
### Constraints (factors that decrease suitability)
- **Non-residential uses** — commercial/industrial lots are harder to consolidate residentially
- **Sensitive interfaces** — adjacent to schools, heritage sites, etc.
- **Restrictive overlays** — heritage, flood, environmental significance overlays limit development
- **Physical constraints** — easements, steep slopes, or difficult topography

***
## Complete Dataset Inventory
Below is every dataset you'll need, with direct links and an explanation of how each fits into the analysis.
### Primary Dataset: Vicmap Property — Parcel Polygon with Parcel Detail
This is the **foundation dataset**. It contains polygon geometries for every land parcel in Victoria with associated identifiers (lot/plan, SPI, LGA code, freehold/crown status).[^2][^7]

- **Download:** [https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail](https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail)[^7]
- **Formats:** SHP, GDB, TAB, DWG, WMS, WFS
- **License:** Creative Commons Attribution 4.0
- **Use for:** Computing lot area, perimeter, shape indices, edge topology, shared boundaries, frontage analysis
### Planning Zones: Vicmap Planning — Zone Polygon
Contains all planning zone boundaries (HCTZ, GRZ, RGZ, NRZ, Commercial, Industrial, etc.) for every Victorian planning scheme.[^8]

- **Download:** [https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-zone-polygon](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-zone-polygon)[^8]
- **Formats:** SHP, GDB, TAB, WMS, WFS
- **License:** CC BY 4.0, updated weekly
- **Use for:** Identifying which parcels fall within HCTZ (or other residential zones), filtering non-residential zones as constraints
### Planning Overlays: Vicmap Planning — Overlay Polygon
Contains all overlay boundaries — Heritage Overlay (HO), Flood overlays (LSIO, SBO), Environmental Significance Overlay (ESO), Vegetation Protection Overlay (VPO), Design and Development Overlay (DDO), and more.[^5]

- **Download:** [https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-overlay-polygon](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-overlay-polygon)[^5]
- **Formats:** SHP, GDB, TAB, WMS, WFS
- **License:** CC BY 4.0, updated weekly
- **Use for:** Flagging parcels with restrictive overlays as consolidation constraints (heritage, flood, environmental significance)
### Full Planning Bundle: Vicmap Planning (Combined)
A combined download of all zone and overlay data together.[^9]

- **Download:** [https://discover.data.vic.gov.au/dataset/vicmap-planning](https://discover.data.vic.gov.au/dataset/vicmap-planning)[^9]
- **Formats:** SHP, GDB, TAB
- **License:** CC BY 4.0
### Public Transport Stops
GeoJSON dataset of **every public transport stop** in Victoria, classified by mode (Metro Train, Metro Tram, Metro Bus, Regional Bus, etc.).[^10]

- **Download:** [https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops](https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops)[^11]
- **Format:** GeoJSON
- **License:** CC BY 4.0
- **Use for:** Computing proximity of each parcel to PT nodes (800m from train/tram, 400m from bus stops), which directly determines HCTZ eligibility
### GTFS Schedule (Public Transport Timetables)
Static timetable data for all Victorian public transport services.[^12]

- **Download:** [https://opendata.transport.vic.gov.au/dataset/gtfs-schedule](https://opendata.transport.vic.gov.au/dataset/gtfs-schedule)[^12]
- **Format:** GTFS (CSV-based standard)
- **License:** CC BY 4.0
- **Use for:** Determining service frequency at each stop (to identify "high-frequency" bus routes for HCTZ proximity calculations)
### Road Network: Vicmap Transport — Road Line
The statewide road network with road classification (freeway, arterial, sub-arterial, collector, local, etc.).[^13]

- **Download:** [https://discover.data.vic.gov.au/dataset/vicmap-transport-road-line](https://discover.data.vic.gov.au/dataset/vicmap-transport-road-line)[^13]
- **Formats:** SHP, GDB, TAB, WMS, WFS
- **License:** CC BY 4.0
- **Use for:** Classifying parcel frontages by road type (arterial vs local vs laneway); identifying access points
### Address Points: Vicmap Address
Georeferenced address points linked to property parcels.[^14]

- **Download:** [https://discover.data.vic.gov.au/dataset/vicmap-address-address-point](https://discover.data.vic.gov.au/dataset/vicmap-address-address-point)[^15]
- **Formats:** SHP, GDB, TAB, WMS, WFS
- **License:** CC BY 4.0
- **Use for:** Linking parcels to addresses; identifying land use through address records; cross-referencing with property data
### Activity Centres: Plan Melbourne Spatial Data
Contains boundaries of Metropolitan, Major, and Neighbourhood Activity Centres across Melbourne.[^16]

- **Download:** [https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room](https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room)[^17]
- **Formats:** ESRI Shapefile and MapInfo TAB
- **License:** Free download
- **Use for:** Measuring proximity of parcels to Activity Centres (a key "opportunity" factor for consolidation)
### Elevation / Topography: Vicmap Elevation
LiDAR-derived Digital Elevation Models (DEMs) covering 60%+ of Victoria at resolutions from 50 cm to 5 m. A free 30m DEM is also available from CeRDI / Federation University.[^18][^19]

- **Vicmap Elevation REST API (free):** [https://discover.data.vic.gov.au/dataset/vicmap-elevation-rest-api](https://discover.data.vic.gov.au/dataset/vicmap-elevation-rest-api)[^20]
- **30m DEM (free):** [https://data2.cerdi.edu.au/dataset/vvg_vicdem_30m_merged_clipped_3857](https://data2.cerdi.edu.au/dataset/vvg_vicdem_30m_merged_clipped_3857)[^19]
- **Use for:** Deriving slope and topography constraints — steep sites are harder/costlier to develop
### Easement Data
Easement lines are included within the Vicmap Property Simplified dataset, available on Data.Vic.[^2]

- **Access:** Available as part of the Vicmap Property download, or separately as "Vicmap Property - Easement Approved Line" on Data.Vic
- **Use for:** Identifying physical constraints that limit buildable area on a parcel
### Open Space
A comprehensive dataset of open space across metropolitan Melbourne from the Victorian Planning Authority.[^21]

- **Download:** [https://discover.data.vic.gov.au/dataset/open-space](https://discover.data.vic.gov.au/dataset/open-space)[^21]
- **Formats:** CSV, KML, SHP, GeoJSON
- **Use for:** Identifying adjacent public uses (sensitive interfaces); also useful for amenity proximity scoring

***
## Quick-Reference Dataset Table
| Dataset | Portal | Primary Use in Analysis | Format |
|---|---|---|---|
| Vicmap Property – Parcel Polygon[^7] | Data.Vic | Parcel geometry, area, edges, topology | SHP/GDB |
| Vicmap Planning – Zone Polygon[^8] | Data.Vic | Identify HCTZ and residential zones | SHP/GDB |
| Vicmap Planning – Overlay Polygon[^5] | Data.Vic | Heritage, flood, ESO constraints | SHP/GDB |
| PT Stops[^10] | Transport Open Data | Proximity to train/tram/bus | GeoJSON |
| GTFS Schedule[^12] | Transport Open Data | Service frequency for bus routes | GTFS |
| Vicmap Transport – Road Line[^13] | Data.Vic | Road classification for frontage | SHP/GDB |
| Vicmap Address[^15] | Data.Vic | Address-to-parcel linkage | SHP/GDB |
| Plan Melbourne Activity Centres[^17] | Planning.vic.gov.au | Proximity to activity centres | SHP/TAB |
| Vicmap Elevation / 30m DEM[^19] | Data.Vic / CeRDI | Slope and topography constraints | GeoTIFF |
| Open Space[^21] | Data.Vic | Sensitive interfaces, amenity | SHP/GeoJSON |

***
## Recommended Workflow
1. **Start with Vicmap Property parcel polygons** — load these into your GIS/Python environment. This gives you every land parcel's geometry.
2. **Overlay Vicmap Planning zones** — spatial join to tag each parcel with its zone (focus on HCTZ, GRZ, RGZ).
3. **Overlay Vicmap Planning overlays** — spatial join to flag parcels affected by HO, LSIO, SBO, ESO, etc.
4. **Compute geometric features** — lot area, perimeter, compactness, aspect ratio, width measurements from the parcel polygons.
5. **Compute edge topology** — intersect parcel boundaries with roads (Vicmap Transport) and adjacent parcels to determine frontage types, shared boundaries, corner-lot status.
6. **Compute proximity features** — distance from each parcel centroid to nearest PT stop, nearest activity centre.
7. **Derive slope** — sample the DEM at each parcel to get average/max slope.
8. **Build scoring model** — combine all features into a multi-criteria score (opportunities minus constraints) to rank parcels by consolidation potential.

All datasets listed above are freely available under Creative Commons licensing from Data.Vic ([https://discover.data.vic.gov.au](https://discover.data.vic.gov.au)), the Transport Open Data portal ([https://opendata.transport.vic.gov.au](https://opendata.transport.vic.gov.au)), and the Planning Victoria website.[^22][^11][^17]

---

## References

1. [Housing Choice and Transport Zone (HCTZ) Development ...](https://landchecker.com.au/functionalities/housing-choice-and-transport-zone-hctz-development-in-victoria/) - In this article, we'll look at what the HCTZ is, how it supports Victoria's planning goals, and what...

2. [Vicmap Property](https://www.land.vic.gov.au/maps-and-spatial/spatial-data/vicmap-catalogue/vicmap-property) - Vicmap Property is Victoria's cadastral map base that provides information about land parcels and pr...

3. [Victorian Government Activity Centres Program](https://tract.com.au/victorian-government-activity-centres-program/) - The ten activity centres identified across Melbourne are a mix of metropolitan and major activity ce...

4. [Activity centres - planning guidance](https://www.planning.vic.gov.au/guides-and-resources/guides/all-guides/activity-centres-guidance) - Metropolitan Melbourne has a network of activity centres and this includes: Metropolitan activity ce...

5. [Vicmap Planning - Planning Scheme Overlay Polygon - Dataset](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-overlay-polygon) - This dataset contains polygon features representing overlay controls for all Victorian planning sche...

6. [Environmental Significance Overlays (ESOs)](https://landchecker.com.au/functionalities/environmental-significance-overlay/) - Environmental Significance Overlays (ESOs) are critical tools used in urban planning and environment...

7. [Vicmap Property - Parcel Polygon with Parcel Detail - Dataset](https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail) - A modified and simplified model of Vicmap Property. It consists of polygons representing Victoria's ...

8. [Vicmap Planning - Planning Scheme Zone Polygon - Dataset](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-zone-polygon) - This dataset contains polygon features representing land use zones (such as residential, industrial ...

9. [Vicmap Planning - Dataset](https://discover.data.vic.gov.au/dataset/vicmap-planning) - Vicmap Planning is the map data representing the land use zone and overlay controls for all Victoria...

10. [Public Transport Lines and Stops](https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops/resource/afa7b823-0c8b-47a1-bc40-ada565f684c7) - Stop locations for every public transport mode in Victoria.

11. [Public Transport Lines and Stops - Data Collection](https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops) - Location of public transport lines and stops per mode in Victoria.

12. [GTFS Schedule - Data Collection](https://opendata.transport.vic.gov.au/dataset/gtfs-schedule) - The GTFS Schedule dataset contains static timetable information of public transport services in Vict...

13. [Vicmap Transport - Road Line - Dataset](https://discover.data.vic.gov.au/dataset/vicmap-transport-road-line) - This layer is part of Vicmap Transport and is and extensive digital road network - line features del...

14. [Vicmap Address](https://www.land.vic.gov.au/maps-and-spatial/spatial-data/vicmap-catalogue/vicmap-address) - Vicmap Address can be used to map the location of addressed assets or to verify the content of busin...

15. [Vicmap Address - Address Point - Dataset](https://discover.data.vic.gov.au/dataset/vicmap-address-address-point) - Vicmap Address is Victoria's authoritative geocoded database of property address points. Data and Re...

16. [Plan Melbourne Spatial Data Source List](https://www.planning.vic.gov.au/__data/assets/pdf_file/0021/628500/plan-melbourne-spatial-data-source-list.pdf) - DELWP - Plan Melbourne (Activity Centres) www.planmelbourne.vic.gov.au/maps. Activity Centres. Exisi...

17. [The map room - Planning](https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room) - Spatial data downloads. The maps on Plan Melbourne tell an important story. Map downloads. View and ...

18. [Vicmap Elevation](https://www.land.vic.gov.au/maps-and-spatial/spatial-data/vicmap-catalogue/vicmap-elevation) - Vicmap Elevation Surface products comprise of data that are modelled to represent continuous height ...

19. [Digital Elevation Model - Victoria (30 meter approx) - grid values](https://data2.cerdi.edu.au/dataset/vvg_vicdem_30m_merged_clipped_3857) - 30 metre Digital Elevation Model (DEM). This layer was merged, clipped and reprojected by CeRDI (Fed...

20. [Vicmap Elevation REST API - Dataset](https://data.gov.au/data/dataset/vicmap-elevation-rest-api) - Vicmap Elevation includes 5 products: Digital Elevation Model (DEM) 10m 1-5m Contours & Relief Melbo...

21. [Open Space - Dataset - Victorian Government Data Vic](https://discover.data.vic.gov.au/dataset/open-space) - A comprehensive GIS dataset to represent the existing open space network throughout the metropolitan...

22. [Datashare - A search and discovery tool that enables ...](https://datashare.maps.vic.gov.au) - DataShare is the place to search and download spatial data managed by the Department of Energy, Envi...

