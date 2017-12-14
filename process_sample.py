print "loaded process_sample"

def process_sample(record):
    if "bin.png" not in record: return None
    if "lines.png" not in record: return None
    return {"__key__": record["__key__"],
            "png": record["bin.png"],
            "lines.png": record["lines.png"]}

