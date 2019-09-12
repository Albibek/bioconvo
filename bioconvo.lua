function handle(name, value, timestamp)
    log("name="..name.."; value="..value.."; timestamp="..timestamp)

    store("some", name, value, timestamp)
 
    if value % 2 == 1 then
        return {"backend1"}
    else
        return {"backend2"}
    end
end
