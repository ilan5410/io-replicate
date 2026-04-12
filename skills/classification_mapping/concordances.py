"""
Bundled concordance tables.

Each concordance is a list of (from_code, from_label, to_code, to_label) tuples.
Data sources:
  NACE_R2_64 labels    — Eurostat RAMON
  CPA_2008 labels      — Eurostat RAMON (FIGARO industry_list)
  ISIC_R4 labels       — UNSD
  WIOD56 labels        — Timmer et al. (2015) WIOD release
  EXIOBASE163 labels   — Exiobase 3.x sector list
"""

# ── NACE Rev. 2 (64-activity detail) ─────────────────────────────────────────
# Code → label (leaf activities only, matching nama_10_a64_e NACE codes)
NACE_R2_64: dict[str, str] = {
    "A01": "Crop and animal production, hunting and related service activities",
    "A02": "Forestry and logging",
    "A03": "Fishing and aquaculture",
    "B05-09": "Mining and quarrying",
    "C10-12": "Manufacture of food products; beverages and tobacco products",
    "C13-15": "Manufacture of textiles, wearing apparel and leather products",
    "C16": "Manufacture of wood and of products of wood and cork",
    "C17": "Manufacture of paper and paper products",
    "C18": "Printing and reproduction of recorded media",
    "C19": "Manufacture of coke and refined petroleum products",
    "C20": "Manufacture of chemicals and chemical products",
    "C21": "Manufacture of basic pharmaceutical products",
    "C22": "Manufacture of rubber and plastic products",
    "C23": "Manufacture of other non-metallic mineral products",
    "C24": "Manufacture of basic metals",
    "C25": "Manufacture of fabricated metal products",
    "C26": "Manufacture of computer, electronic and optical products",
    "C27": "Manufacture of electrical equipment",
    "C28": "Manufacture of machinery and equipment n.e.c.",
    "C29": "Manufacture of motor vehicles, trailers and semi-trailers",
    "C30": "Manufacture of other transport equipment",
    "C31-32": "Manufacture of furniture; other manufacturing",
    "C33": "Repair and installation of machinery and equipment",
    "D35": "Electricity, gas, steam and air conditioning supply",
    "E36": "Water collection, treatment and supply",
    "E37-39": "Sewerage; waste collection, treatment and disposal; remediation activities",
    "F41-43": "Construction",
    "G45": "Wholesale and retail trade and repair of motor vehicles",
    "G46": "Wholesale trade, except of motor vehicles",
    "G47": "Retail trade, except of motor vehicles",
    "H49": "Land transport and transport via pipelines",
    "H50": "Water transport",
    "H51": "Air transport",
    "H52": "Warehousing and support activities for transportation",
    "H53": "Postal and courier activities",
    "I55-56": "Accommodation and food service activities",
    "J58": "Publishing activities",
    "J59-60": "Motion picture, video and TV; sound recording and music publishing",
    "J61": "Telecommunications",
    "J62-63": "Computer programming, consultancy and information service activities",
    "K64": "Financial service activities, except insurance and pension funding",
    "K65": "Insurance, reinsurance and pension funding",
    "K66": "Activities auxiliary to financial services and insurance",
    "L68": "Real estate activities",
    "M69-70": "Legal and accounting; management consultancy activities",
    "M71": "Architectural and engineering activities; technical testing and analysis",
    "M72": "Scientific research and development",
    "M73": "Advertising and market research",
    "M74-75": "Other professional, scientific and technical activities; veterinary",
    "N77": "Rental and leasing activities",
    "N78": "Employment activities",
    "N79": "Travel agency and tour operator activities",
    "N80-82": "Security and investigation; services to buildings; business support",
    "O84": "Public administration and defence; compulsory social security",
    "P85": "Education",
    "Q86": "Human health activities",
    "Q87-88": "Residential care; social work activities without accommodation",
    "R90-92": "Creative, arts and entertainment; libraries, museums, other cultural",
    "R93": "Sports activities and amusement and recreation activities",
    "S94": "Activities of membership organisations",
    "S95": "Repair of computers and personal and household goods",
    "S96": "Other personal service activities",
    "T97-98": "Activities of households as employers",
    "U99": "Activities of extraterritorial organisations and bodies",
}

# ── CPA 2008 (64-product detail, matching FIGARO spec codes after _norm_cpa) ──
CPA_2008_64: dict[str, str] = {
    "A01": "Products of agriculture, hunting and related services",
    "A02": "Products of forestry, logging and related services",
    "A03": "Fish and other fishing products; aquaculture products",
    "B05-09": "Mining and quarrying",
    "C10-12": "Food products, beverages and tobacco products",
    "C13-15": "Textiles, wearing apparel and leather products",
    "C16": "Wood and products of wood and cork",
    "C17": "Paper and paper products",
    "C18": "Printing and recording services",
    "C19": "Coke and refined petroleum products",
    "C20": "Chemicals and chemical products",
    "C21": "Basic pharmaceutical products and pharmaceutical preparations",
    "C22": "Rubber and plastic products",
    "C23": "Other non-metallic mineral products",
    "C24": "Basic metals",
    "C25": "Fabricated metal products",
    "C26": "Computer, electronic and optical products",
    "C27": "Electrical equipment",
    "C28": "Machinery and equipment n.e.c.",
    "C29": "Motor vehicles, trailers and semi-trailers",
    "C30": "Other transport equipment",
    "C31-32": "Furniture; other manufactured goods",
    "C33": "Repair and installation services of machinery and equipment",
    "D35": "Electricity, gas, steam and air conditioning",
    "E36": "Natural water; water treatment and supply services",
    "E37-39": "Sewerage; waste collection, treatment and disposal",
    "F41-43": "Constructions and construction works",
    "G45": "Wholesale and retail trade and repair of motor vehicles",
    "G46": "Wholesale trade services",
    "G47": "Retail trade services",
    "H49": "Land transport services and transport via pipelines",
    "H50": "Water transport services",
    "H51": "Air transport services",
    "H52": "Warehousing and support services for transportation",
    "H53": "Postal and courier services",
    "I55-56": "Accommodation and food services",
    "J58": "Publishing services",
    "J59-60": "Motion picture, video, TV programme production; broadcasting",
    "J61": "Telecommunications services",
    "J62-63": "Computer programming, consultancy and information services",
    "K64": "Financial services",
    "K65": "Insurance, reinsurance and pension funding services",
    "K66": "Services auxiliary to financial services and insurance",
    "L68": "Real estate services",
    "M69-70": "Legal and accounting; management consulting services",
    "M71": "Architectural and engineering services; technical testing",
    "M72": "Scientific research and development services",
    "M73": "Advertising and market research services",
    "M74-75": "Other professional, scientific and technical services",
    "N77": "Rental and leasing services",
    "N78": "Employment services",
    "N79": "Travel agency, tour operator and other reservation services",
    "N80-82": "Security; services to buildings; business support services",
    "O84": "Public administration and defence; social security",
    "P85": "Education services",
    "Q86": "Human health services",
    "Q87-88": "Residential care services and social work services",
    "R90-92": "Creative, arts and entertainment; libraries, museums; gambling",
    "R93": "Sports services and amusement and recreation services",
    "S94": "Services furnished by membership organisations",
    "S95": "Repair services of computers and personal and household goods",
    "S96": "Other personal services",
    "T97-98": "Services of households as employers",
    "U99": "Services of extraterritorial organisations",
}

# ── ISIC Rev. 4 (section/division labels, most common granularity in papers) ──
ISIC_R4: dict[str, str] = {
    "A01": "Crop and animal production, hunting and related service activities",
    "A02": "Forestry and logging",
    "A03": "Fishing and aquaculture",
    "B": "Mining and quarrying",
    "C10": "Manufacture of food products",
    "C11": "Manufacture of beverages",
    "C12": "Manufacture of tobacco products",
    "C13": "Manufacture of textiles",
    "C14": "Manufacture of wearing apparel",
    "C15": "Manufacture of leather and related products",
    "C16": "Manufacture of wood and products of wood and cork",
    "C17": "Manufacture of paper and paper products",
    "C18": "Printing and reproduction of recorded media",
    "C19": "Manufacture of coke and refined petroleum products",
    "C20": "Manufacture of chemicals and chemical products",
    "C21": "Manufacture of pharmaceutical products",
    "C22": "Manufacture of rubber and plastics products",
    "C23": "Manufacture of other non-metallic mineral products",
    "C24": "Manufacture of basic metals",
    "C25": "Manufacture of fabricated metal products",
    "C26": "Manufacture of computer, electronic and optical products",
    "C27": "Manufacture of electrical equipment",
    "C28": "Manufacture of machinery and equipment n.e.c.",
    "C29": "Manufacture of motor vehicles, trailers and semi-trailers",
    "C30": "Manufacture of other transport equipment",
    "C31": "Manufacture of furniture",
    "C32": "Other manufacturing",
    "C33": "Repair and installation of machinery and equipment",
    "D": "Electricity, gas, steam and air conditioning supply",
    "E36": "Water collection, treatment and supply",
    "E37": "Sewerage",
    "E38": "Waste collection, treatment and disposal",
    "E39": "Remediation activities",
    "F": "Construction",
    "G45": "Wholesale and retail trade and repair of motor vehicles",
    "G46": "Wholesale trade",
    "G47": "Retail trade",
    "H49": "Land transport and transport via pipelines",
    "H50": "Water transport",
    "H51": "Air transport",
    "H52": "Warehousing and support activities for transportation",
    "H53": "Postal and courier activities",
    "I": "Accommodation and food service activities",
    "J58": "Publishing activities",
    "J59": "Motion picture, video and television programme production",
    "J60": "Programming and broadcasting activities",
    "J61": "Telecommunications",
    "J62": "Computer programming, consultancy and related activities",
    "J63": "Information service activities",
    "K64": "Financial service activities",
    "K65": "Insurance, reinsurance and pension funding",
    "K66": "Activities auxiliary to financial service activities",
    "L": "Real estate activities",
    "M69": "Legal and accounting activities",
    "M70": "Activities of head offices; management consultancy",
    "M71": "Architectural and engineering activities",
    "M72": "Scientific research and development",
    "M73": "Advertising and market research",
    "M74": "Other professional, scientific and technical activities",
    "M75": "Veterinary activities",
    "N77": "Rental and leasing activities",
    "N78": "Employment activities",
    "N79": "Travel agency and tour operator activities",
    "N80": "Security and investigation activities",
    "N81": "Services to buildings and landscape activities",
    "N82": "Office administrative and business support activities",
    "O": "Public administration and defence; compulsory social security",
    "P": "Education",
    "Q86": "Human health activities",
    "Q87": "Residential care activities",
    "Q88": "Social work activities without accommodation",
    "R90": "Creative, arts and entertainment activities",
    "R91": "Libraries, archives, museums and other cultural activities",
    "R92": "Gambling and betting activities",
    "R93": "Sports activities and amusement and recreation activities",
    "S94": "Activities of membership organisations",
    "S95": "Repair of computers and personal and household goods",
    "S96": "Other personal service activities",
    "T": "Activities of households as employers",
    "U": "Activities of extraterritorial organisations",
}

# ── WIOD 2016 (56-sector codes) ───────────────────────────────────────────────
# Timmer et al. (2015) — The World Input-Output Database (WIOD): Contents, Sources and Methods
WIOD56: dict[str, str] = {
    "A01": "Crop and animal production, hunting and related service activities",
    "A02": "Forestry and logging",
    "A03": "Fishing and aquaculture",
    "B": "Mining and quarrying",
    "C10-C12": "Manufacture of food products; beverages and tobacco",
    "C13-C15": "Manufacture of textiles, wearing apparel and leather products",
    "C16": "Manufacture of wood and products of wood",
    "C17": "Manufacture of paper and paper products",
    "C18": "Printing and reproduction of recorded media",
    "C19": "Manufacture of coke and refined petroleum products",
    "C20": "Manufacture of chemicals and chemical products",
    "C21": "Manufacture of pharmaceutical products",
    "C22": "Manufacture of rubber and plastics products",
    "C23": "Manufacture of other non-metallic mineral products",
    "C24": "Manufacture of basic metals",
    "C25": "Manufacture of fabricated metal products",
    "C26": "Manufacture of computer, electronic and optical products",
    "C27": "Manufacture of electrical equipment",
    "C28": "Manufacture of machinery and equipment n.e.c.",
    "C29": "Manufacture of motor vehicles, trailers and semi-trailers",
    "C30": "Manufacture of other transport equipment",
    "C31_C32": "Manufacture of furniture; other manufacturing",
    "C33": "Repair and installation of machinery and equipment",
    "D35": "Electricity, gas, steam and air conditioning supply",
    "E36": "Water collection, treatment and supply",
    "E37-E39": "Sewerage, waste collection, treatment and disposal",
    "F": "Construction",
    "G45": "Wholesale and retail trade and repair of motor vehicles",
    "G46": "Wholesale trade, except of motor vehicles and motorcycles",
    "G47": "Retail trade, except of motor vehicles and motorcycles",
    "H49": "Land transport and transport via pipelines",
    "H50": "Water transport",
    "H51": "Air transport",
    "H52": "Warehousing and support activities for transportation",
    "H53": "Postal and courier activities",
    "I": "Accommodation and food service activities",
    "J58": "Publishing activities",
    "J59_J60": "Motion picture, video and television programme production",
    "J61": "Telecommunications",
    "J62_J63": "Computer programming, consultancy and information service activities",
    "K64": "Financial service activities, except insurance and pension funding",
    "K65": "Insurance, reinsurance and pension funding",
    "K66": "Activities auxiliary to financial service and insurance activities",
    "L68": "Real estate activities",
    "M69_M70": "Legal and accounting activities; management consultancy",
    "M71": "Architectural and engineering activities; technical testing",
    "M72": "Scientific research and development",
    "M73": "Advertising and market research",
    "M74_M75": "Other professional, scientific and technical activities; veterinary",
    "N77": "Rental and leasing activities",
    "N78": "Employment activities",
    "N79": "Travel agency and related activities",
    "N80-N82": "Security, services to buildings, business support activities",
    "O84": "Public administration and defence; compulsory social security",
    "P85": "Education",
    "Q86-Q88": "Human health and social work activities",
    "R_S": "Arts, entertainment, recreation; other service activities",
    "T": "Activities of households as employers",
    "U": "Activities of extraterritorial organisations",
}

# ── Registered concordances ────────────────────────────────────────────────────
# Maps (from_cls, to_cls) → function that returns list of (from_code, to_code) pairs

def _nace_to_cpa() -> list[tuple[str, str]]:
    """NACE Rev. 2 64 → CPA 2008 64  (1-to-1 at this aggregation level)."""
    # At the FIGARO 64-sector level, NACE and CPA are structurally identical
    # (same letter-digit codes).  The mapping is direct code equality.
    nace_codes = set(NACE_R2_64.keys())
    cpa_codes = set(CPA_2008_64.keys())
    common = nace_codes & cpa_codes
    return [(c, c) for c in sorted(common)]


def _nace_to_isic() -> list[tuple[str, str]]:
    """NACE Rev. 2 → ISIC Rev. 4  (NACE is a European elaboration of ISIC)."""
    # Direct code matches (NACE codes are generally ISIC codes with European splits)
    # Approximate 1-to-1 for sections/divisions used in most IO papers
    # Full concordance: https://unstats.un.org/unsd/classifications/Econ/isic
    pairs = []
    for nace_code in NACE_R2_64:
        # Most NACE section codes map directly (A01→A01, C20→C20, etc.)
        if nace_code in ISIC_R4:
            pairs.append((nace_code, nace_code))
        else:
            # Range codes: C10-12 maps to C10+C11+C12
            prefix = nace_code.split("-")[0] if "-" in nace_code else nace_code
            matches = [k for k in ISIC_R4 if k.startswith(prefix)]
            for m in matches:
                pairs.append((nace_code, m))
    return pairs


def _nace_to_wiod() -> list[tuple[str, str]]:
    """NACE Rev. 2 64 → WIOD 56  (1-to-1 at most positions)."""
    nace_codes = set(NACE_R2_64.keys())
    wiod_codes = set(WIOD56.keys())
    # Normalize separators for matching
    def _norm(c):
        return c.replace("_", "-")
    wiod_norm = {_norm(c): c for c in wiod_codes}
    pairs = []
    for nc in nace_codes:
        if _norm(nc) in wiod_norm:
            pairs.append((nc, wiod_norm[_norm(nc)]))
    return pairs


REGISTERED: dict[tuple[str, str], callable] = {
    ("NACE_R2_64", "CPA_2008_64"): _nace_to_cpa,
    ("NACE_R2_64", "ISIC_R4"):     _nace_to_isic,
    ("NACE_R2_64", "WIOD56"):      _nace_to_wiod,
}

CLASSIFICATION_LABELS: dict[str, dict[str, str]] = {
    "NACE_R2_64":  NACE_R2_64,
    "CPA_2008_64": CPA_2008_64,
    "ISIC_R4":     ISIC_R4,
    "WIOD56":      WIOD56,
}
