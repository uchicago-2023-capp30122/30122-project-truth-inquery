import requests

apikey = 'aKCLywC5672'

relative_rls = ["/v1/gestational_limits/states/", 
                "/v1/insurance_coverage/states/", 
                "/v1/minors/states/", 
                "/v1/waiting_periods/states/"]

url = 'http://api.abortionpolicyapi.com/v1/gestational_limits/states'
headers = { 'token': apikey }
r = requests.get(url, headers=headers)

states = r.json()

limit_at_viability = [state for state in states.keys() if states[state].get('banned_after_weeks_since_LMP') == 99]
limit_at_viability.sort()

message = f"The states that ban abortion at viability are: {', '.join(limit_at_viability)}"
print(message)