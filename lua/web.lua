local function errorf(fmt, ...)
    error(string.format(fmt, ...))
end



local function extend(tbl, tblu, raise)
    local res = {}
    for k, v in pairs(tbl) do
        res[k] = v
    end
    for k, v in pairs(tblu) do
        if raise then
            if res[k] == nil then
                errorf("Unknown option '%s'", k)
            end
        end
        res[k] = v
    end
    return res
end

local function add_handler(self, type, name_handler, handler)
    if type == "http" then
        self.handlers_http[name_handler] = handler
    elseif type == "ws" then
        self.handlers_ws[name_handler] = handler
    else
        --- -TODO
    end
    return self
end

local function add_handler_ws(self, type, name_handler, handler)
    add_handler(self,"ws",name_handler,handler)
    return self
end


local function add_handler_http(self, type, name_handler, handler)
    print("edc")
    add_handler(self,"http",name_handler,handler)
    return self
end

local function find_handler(self, type, name_handler)
    if type == "http" then
        print("1234567890")
        return self.handlers_http[name_handler]
    elseif type == "ws" then
        return self.handlers_ws[name_handler]
    else
        --- -TODO
    end
end

local function find_ws_handler(self, name_handler)
    return find_handler(self,"ws",name_handler)
end

local function find_http_handler(self, name_handler)
    print(4567)
    return find_handler(self,"http",name_handler)
end



local function dispatcher(self, id)
    local request = {}
    request["id"] = id
    job_read(id, request)
    if request["type"] == "http" then
        return self.dispatchers["http"](self, request)
    elseif request["type"] == "ws" then
        return self.dispatchers["ws"](self, request)
    else
        --- -TODO
        return
    end

    return

end

local function start(self)
    if type(self) ~= 'table' then
        error("httpd: usage: httpd:start()")
    end

    while true do
        local jobs = {}
        jobs_wait(jobs)
        for key in pairs(jobs) do
            dispatcher(self, jobs[key])
        end
        for key in pairs(jobs) do
            jobs[key] = nil
        end
    end
end

local function add_dispatcher(self, type, handler)
    self.dispatchers[type] = handler
    return self
end

local function write_and_close(self,response)
    print("test response")
    print(response["body"])
    job_write_and_close(response)

end


local exports = {
    application = function()

        local default = {}

        local self = {
            job_id = {},

            --- methods
            handler_ws = add_handler_ws,
            handler_http = add_handler_http,
            ws = find_ws_handler,
            http =find_http_handler,
            dispatcher = add_dispatcher,
            start = start,
            write_and_close = write_and_close,
            --- handler
            dispatchers = {},
            handlers_http = {},
            handlers_ws = {}
        }

        return self
    end
}

return exports