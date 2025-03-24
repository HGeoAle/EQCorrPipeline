import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def create_catalog_file(catalog, id_map_dict, filename='event_file.txt'):
    with open(filename, 'w') as file:
        # Growclust Crashes if the header is included
        #file.write("YEAR,MONTH,DAY,HOURs,MINUTEs,SECONDS,ID,Y,X,Z,ML\n")
        for event in catalog:
            # Get event origin
            origin = event.origins[0]
            
            # Get event time
            event_time = origin.time
            year = event_time.year
            month = event_time.month
            day = event_time.day
            hour = event_time.hour
            minute = event_time.minute
            second = event_time.second + event_time.microsecond / 1e6
            
            # Get event simple ID
            resource_id = str(event.resource_id)
            simple_id = id_map_dict.get(resource_id, 'UnknownID')
            
            # Get latitude, longitude, depth
            latitude = origin.latitude
            longitude = origin.longitude
            depth = origin.depth  # Depth is already in km
            
            # Get magnitude (assuming there is at least one magnitude)
            if not event.magnitudes:
                print(f"No magnitude for event {resource_id}. Skipping this event.")
                continue
            magnitude = event.magnitudes[0].mag
            
            # Write line to file
            file.write(f"{year} {month:02d} {day:02d} {hour:02d} {minute:02d} {second:06.3f} {simple_id} {latitude:.4f} {longitude:.4f} {depth:.2f} {magnitude:.3f}\n")