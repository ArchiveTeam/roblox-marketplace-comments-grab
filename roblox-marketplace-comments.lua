local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local base64 = require("base64")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local thread_counts = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https?://catalog%.roblox%.com/v1/catalog/items/([0-9]+)/details%?itemType=Asset$"]="asset",
    ["^https?://users%.roblox%.com/v1/users/([0-9]+)$"]="user",
    ["^https?://groups%.roblox%.com/v1/groups/([0-9]+)$"]="group",
    ["^https?://www%.roblox%.com/comments/get%-json%?assetId=([0-9]+)&startindex=0&extra=badge$"]="badge"
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[item_value] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  if string.match(url, "^https?://[^/]+/login%?")
    or string.match(url, "^https?://[^/]+/[nN]ew[lL]ogin%?")
    or string.match(url, "^https?://avatar%.roblox%.com/v1/avatar/assets/[0-9]+/wear$")
    or string.match(url, "^https?://avatar%.roblox%.com/v1/avatar/assets/[0-9]+/remove$")
    or string.match(url, "^https?://[^/]+/abusereport/")
    or string.match(url, "^https?://www%.roblox%.com/[a-z][a-z]/catalog/")
    or string.match(url, "^https?://www%.roblox%.com/[a-z][a-z]/users/")
    or string.match(url, "^https?://www%.roblox%.com/[a-z][a-z]/groups/")
    or string.match(url, "^https?://www%.roblox%.com/messages/compose%?") then
    return false
  end

  if string.match(url, "^https?://[^/]*roblox%.com/") then
    for _, pattern in pairs({
      "([0-9]+)"
    }) do
      for s in string.gmatch(url, pattern) do
        if ids[s] then
          return true
        end
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
--print('queued', url_)
      table.insert(urls, {
        url=url_
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      return set_new_params(newurl, {[param]=tostring(default)})
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local function check_cursor(newurl, json, cursor_key)
    local cursor = json[cursor_key]
    if cursor ~= cjson.null then
      check(set_new_params(url, {["cursor"]=cursor}))
    end
  end

  if allowed(url)
    and status_code < 300 then
    html = read_file(file)

    -- badge
    if string.match(url, "/comments/get%-json%?assetId=[0-9]+&startindex=0&extra=badge$") then
      check("https://www.roblox.com/comments/get-json?assetId=" .. item_value .. "&startindex=0")
      return urls
    end

    -- asset
    if string.match(url, "^https?://catalog%.roblox%.com/v1/catalog/items/[0-9]+/details%?itemType=Asset$") then
      json = cjson.decode(html)
      local creator_id = json["creatorId"] or json["creatorTargetId"]
      discover_item(discovered_items, string.lower(json["creatorType"]) .. ":" .. tostring(creator_id))
      check("https://www.roblox.com/catalog/" .. item_value)
      check("https://www.roblox.com/comments/get-json?assetId=" .. item_value .. "&startindex=0&thumbnailWidth=100&thumbnailHeight=100&thumbnailFormat=PNG")
      check("https://catalog.roblox.com/v1/favorites/assets/" .. item_value .. "/count")
      check("https://catalog.roblox.com/v2/recommendations/assets?assetId=" .. item_value .. "&assetTypeId=8&numItems=7")
      check("https://catalog.roblox.com/v2/recommendations/assets?assetId=" .. item_value .. "&assetTypeId=8&numItems=50")
    end
    if string.match(url, "^https?://catalog%.roblox%.com/v2/recommendations/assets%?") then
      json = cjson.decode(html)
      for _, new_id in pairs(json["data"]) do
        discover_item(discovered_items, "asset:" .. tostring(new_id))
      end
    end
    -- (and badge)
    if string.match(url, "^https?://www%.roblox%.com/comments/get%-json%?") then
      json = cjson.decode(html)
      local max_comments = json["MaxRows"]
      local count = 0
      for _, comment_data in pairs(json["Comments"]) do
        count = count + 1
        discover_item(discovered_items, "user:" .. tostring(comment_data["AuthorId"]))
      end
      if count == max_comments then
        check(increment_param(url, "startindex", 0, max_comments))
      end
    end


    --user
    if string.match(url, "^https?://users%.roblox%.com/v1/users/[0-9]+$") then
      check("https://www.roblox.com/users/" .. item_value)
      check("https://www.roblox.com/users/profile/playerassets-json?assetTypeId=10&userId=" .. item_value)
      check("https://www.roblox.com/users/profile/playerassets-json?assetTypeId=11&userId=" .. item_value)
      check("https://groups.roblox.com/v1/users/" .. item_value .. "/groups/roles")
      check("https://friends.roblox.com/v1/users/" .. item_value .. "/friends/count")
      check("https://friends.roblox.com/v1/users/" .. item_value .. "/followings/count")
      check("https://friends.roblox.com/v1/users/" .. item_value .. "/followers/count")
      check("https://games.roblox.com/v2/users/" .. item_value .. "/games")
      check("https://avatar.roblox.com/v1/users/" .. item_value .. "/currently-wearing")
      check("https://badges.roblox.com/v1/users/" .. item_value .. "/badges?sortOrder=Desc")
      check("https://accountinformation.roblox.com/v1/users/" .. item_value .. "/roblox-badges")
      check("https://groups.roblox.com/v1/users/" .. item_value .. "/groups/primary/role")
      check("https://www.roblox.com/users/" .. item_value .. "/favorites")
      check("https://www.roblox.com/users/" .. item_value .. "/friends")
    end
    if string.match(url, "^https?://www%.roblox%.com/users/[0-9]+/friends$") then
      check("https://friends.roblox.com/v1/metadata?targetUserId=" .. item_value)
      check("https://friends.roblox.com/v1/users/" .. item_value .. "/followings?sortOrder=Desc&limit=18")
      check("https://friends.roblox.com/v1/users/" .. item_value .. "/followers?sortOrder=Desc&limit=18")
    end
    if string.match(url, "/v1/users/[0-9]+/follow[a-z]+%?") then
      json = cjson.decode(html)
      check_cursor(url, json, "nextPageCursor")
      for _, data in pairs(json["data"]) do
        discover_item(discovered_items, "user:" .. tostring(data["id"]))
      end
    end
    if string.match(url, "^https?://www%.roblox%.com/users/[0-9]+/favorites$") then
      check("https://inventory.roblox.com/v1/users/" .. item_value .. "/categories/favorites")
    end
    if string.match(url, "/v1/users/[0-9]+/categories/favorites$") then
      json = cjson.decode(html)
      for _, data in pairs(json["categories"]) do
        for _, item_data in pairs(data["items"]) do
          if item_data["type"] == "AssetType" then
            check("https://www.roblox.com/users/favorites/list-json?assetTypeId=" .. tostring(item_data["id"]) .. "&cursor=&itemsPerPage=100&userId=" .. item_value)
          end
        end
      end
    end
    if string.match(url, "/users/favorites/list%-json%?") then
      json = cjson.decode(html)
      for _, data in pairs(json["Data"]["Items"]) do
        discover_item(discovered_items, "asset:" .. tostring(data["Item"]["AssetId"]))
        discover_item(discovered_items, "user:" .. tostring(data["Creator"]["Id"]))
      end
      check_cursor(url, json["Data"], "NextCursor")
    end
    if string.match(url, "/v1/users/[0-9]+/groups/roles$") then
      json = cjson.decode(html)
      for _, data in pairs(json["data"]) do
        discover_item(discovered_items, "group:" .. tostring(data["group"]["id"]))
        discover_item(discovered_items, "user:" .. tostring(data["group"]["owner"]["userId"]))
      end
    end
    if string.match(url, "/v2/users/[0-9]+/games$") then
      json = cjson.decode(html)
      for _, data in pairs(json["data"]) do
        discover_item(discovered_items, string.lower(data["creator"]["type"]) .. ":" .. tostring(data["creator"]["id"]))
      end
    end
    if string.match(url, "/v1/users/[0-9]+/currently%-wearing$") then
      json = cjson.decode(html)
      for _, new_id in pairs(json["assetIds"]) do
        discover_item(discovered_items, "asset:" .. tostring(new_id))
      end
    end


    --group
    if string.match(url, "^https?://groups%.roblox%.com/v1/groups/[0-9]+$") then
      check("https://www.roblox.com/groups/" .. item_value)
      check("https://groups.roblox.com/v1/groups/" .. item_value .. "/roles")
      check("https://groups.roblox.com/v1/groups/" .. item_value .. "/membership")
      check("https://groups.roblox.com/v1/groups/" .. item_value .. "/membership?includeNotificationPreferences=true")
      check("https://groups.roblox.com/v1/groups/" .. item_value .. "/name-history")
      check("https://games.roblox.com/v2/groups/" .. item_value .. "/games?accessFilter=Public&cursor=&limit=50&sortOrder=Desc")
      check("https://apis.roblox.com/community-links/v1/groups/" .. item_value .. "/community")
      check("https://groups.roblox.com/v1/groups/" .. item_value .. "/roles/51323954/users?cursor=&limit=50&sortOrder=Desc")
      check("https://groups.roblox.com/v2/groups/" .. item_value .. "/wall/posts?cursor=&limit=50&sortOrder=Desc")
      check("https://catalog.roblox.com/v1/search/items?category=All&creatorTargetId=" .. item_value .. "&creatorType=Group&cursor=&limit=50&sortOrder=Desc&sortType=Updated")
      check("https://groups.roblox.com/v1/groups/" .. item_value .. "/relationships/allies?maxRows=50&sortOrder=Asc&startRowIndex=0")
    end
    if string.match(url, "/v1/groups/[0-9]+/roles/[0-9]+/users%?") then
      json = cjson.decode(html)
      check_cursor(url, json, "nextPageCursor")
      for _, data in pairs(json["data"]) do
        discover_item(discovered_items, "user:" .. tostring(data["userId"]))
      end
    end
    if string.match(url, "/v1/search/items%?") then
      json = cjson.decode(html)
      check_cursor(url, json, "nextPageCursor")
      for _, data in pairs(json["data"]) do
        discover_item(discovered_items, string.lower(data["itemType"]) .. ":" .. tostring(data["id"]))
      end
    end
    if string.match(url, "/v1/groups/[0-9]+/relationships/allies%?") then
      json = cjson.decode(html)
      local count = 0
      for _, data in pairs(json["relatedGroups"]) do
        count = count + 1
        discover_item(discovered_items, "group:" .. tostring(data["id"]))
      end
      if count > 0 then
        check(increment_param(url, "startRowIndex", "0", json["nextRowIndex"]))
      end
    end


    if string.match(html, "^%s*{") then
      if not json then
        json = cjson.decode(html)
      end
      if json["error"] then
        error()
      end
      html = html .. flatten_json(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    if not string.match(url, "%.mpd$") then
      html = string.gsub(html, "&gt;", ">")
      html = string.gsub(html, "&lt;", "<")
      for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
        checknewurl(newurl)
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301
    and http_stat["statcode"] ~= 302 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if seen_200[url["url"]] then
    print("Received data incomplete.")
    abort_item()
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 8
    if status_code == 404 then
      maxtries = 0
    elseif status_code ~= 429 then
      maxtries = 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      seen_200[url["url"]] = true
    end
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["roblox-marketplace-comments-kunfnmnk3etom8k6"] = discovered_items,
    ["urls-qjseqz8p7belr3yx"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


