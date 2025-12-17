// Load this in spark-shell to confirm whether spark.scache.* configs are visible.

println("spark.scache.enable = " + sc.getConf.getOption("spark.scache.enable"))
println("spark.scache.home   = " + sc.getConf.getOption("spark.scache.home"))
println("spark.scache.jars   = " + sc.getConf.getOption("spark.scache.jars"))

System.exit(0)