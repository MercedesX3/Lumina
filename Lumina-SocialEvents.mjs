import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, UpdateCommand, PutCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

const dynamoDB = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoDB, {
    marshallOptions: {
        removeUndefinedValues: true,
    }
});

const tableName = 'CelestialData';
const socialURL = new URL("https://nightsky.jpl.nasa.gov/json/events/api/");

export const handler = async (event) => {

    try{
        
        const scanParams = {
            TableName: tableName,
        };
        const scanResult = await docClient.send(new ScanCommand(scanParams));
        const existingEvents = scanResult.Items || []; 
    
        for(const item of existingEvents){
            const eventDate = item.date;
            const clearParams = {
                TableName: tableName,
                Key: {date: eventDate},
                UpdateExpression: 'SET #eventType.social = :emptyList',
                ExpressionAttributeNames: {'#eventType': 'eventType'},
                ExpressionAttributeValues: {':emptyList': []},
            };
            await docClient.send(new UpdateCommand(clearParams));
        }
        console.log("social events have been cleared");

        const response = await fetch(socialURL, {
            method: 'GET',
        });
    
        if (!response.ok) {
            throw new Error('HTTP error');
        }
    
        const socialEvents = await response.json();
        

        const monthCap = {
            '01': 31,
            '02': 28,
            '03': 31,
            '04': 30,
            '05': 31,
            '06': 30,
            '07': 31,
            '08': 31,
            '09': 30,
            '10': 31,
            '11': 30,
            '12': 31,
        }
        const days = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23' ,'24', '25', '26', '27', '28', '29', '30', '31'];
        const months = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
        
        for(const event of socialEvents.events){
            //fetching valuable information for each event
            const id  = event.event_id;
            const eventUrl = event.partner_opp_url;
            const eventName = event.title;
            const startDate =  event.start_dates[0].substring(0, 10);
            const endDate = event.end_dates[0].substring(0, 10);
            const locationName = event.location_name;
            const streetAddress = event.address_street;
            const cityName = event.address_city;
            const stateName = event.address_state;
            const zipcode = event.address_zip;
            const imgURL = event.image_url;
            
            //creating event
            const newEvent = {
                //type: 'social',
                id: id,
                eventURL: eventUrl,
                eventTitle: eventName,
                location: locationName,
                address: streetAddress,
                city: cityName,
                state: stateName,
                zip: zipcode,
                img: imgURL,
                date: startDate,
            };
            
            const cleanNewEvent = setDefaultValues(newEvent);

            const getParams = {
                TableName: tableName,
                Key: {date : startDate},
        
            };
        
            const result = await docClient.send(new GetCommand(getParams));
            console.log("get command occurred");
        
            if(result.Item){
                // const existingEvents = result.Item.events || [];
                // existingEvents.push(newEvent);
        
                const updateParams = {
                    TableName: tableName,
                    Key: { date: startDate },
                    UpdateExpression: 'SET #eventType.social = list_append(if_not_exists(#eventType.social, :emptyList), :socialEvent)',
                    ExpressionAttributeNames: { '#eventType': 'eventType' },
                    ExpressionAttributeValues: {
                        ':socialEvent': [{
                            id: newEvent.id,
                            eventURL: newEvent.eventURL,
                            eventTitle: newEvent.eventTitle,
                            location: newEvent.location,
                            address: newEvent.address,
                            city: newEvent.city,
                            state: newEvent.state,
                            zip: newEvent.zip,
                            img: newEvent.img,
                            date: newEvent.date,
                        }],
                        ':emptyList': []
                    }
                };
                await docClient.send(new UpdateCommand(updateParams));
                console.log(`Event added to existing date: ${startDate}`);
            }else{
                const putParams = {
                    TableName: tableName,
                    Item: {
                        date: startDate,
                        eventType: {
                            celestial: [],
                            social: [{
                                id: newEvent.id,
                                eventURL: newEvent.eventURL,
                                eventTitle: newEvent.eventTitle,
                                location: newEvent.location,
                                address: newEvent.address,
                                city: newEvent.city,
                                state: newEvent.state,
                                zip: newEvent.zip,
                                img: newEvent.img,
                                date: newEvent.date,
                            }],
                        }
                    }
                };
                await docClient.send(new PutCommand(putParams));
                console.log(`New date created with event: ${startDate}`);
            }

            //if endDate > startDate, then we must enter the event for days in between
            let tempDate = startDate;
            let month = parseInt(startDate.substring(5, 7));
            let cap = monthCap[months[month]];
            let day = parseInt(tempDate.substring(8, 10));
            let year = tempDate.substring(0,4);

            while(tempDate != endDate){
                if(day==cap){
                if(month == 12){
                    month = 1;
                    year = '2025';
                }else{
                    month++;
                }
                day = 1;
                }else{
                    day++;
                }
                tempDate = year + "-" + months[month] + "-" + days[day];
                const tempEvent = {
                    //type: 'social',
                    id: id,
                    eventURL: eventUrl,
                    eventTitle: eventName,
                    location: locationName,
                    address: streetAddress,
                    city: cityName,
                    state: stateName,
                    zip: zipcode,
                    img: imgURL,
                    date: tempDate,
                };

                const cleanTempEvent = setDefaultValues(tempEvent);
                //console.log('Clean Event:', cleanNewEvent);
                const tempGetParams = {
                    TableName: tableName,
                    Key: {date : tempDate},
            
                };
            
                const tempResult = await docClient.send(new GetCommand(tempGetParams));
                console.log("get command occurred");
            
                if(tempResult.Item){
                    // const existingEvents = result.Item.events || [];
                    // existingEvents.push(newEvent);
            
                    const tempUpdateParams = {
                        TableName: tableName,
                        Key: { date: tempDate },
                        UpdateExpression: 'SET #eventType.social = list_append(if_not_exists(#eventType.social, :emptyList), :socialEvent)',
                        ExpressionAttributeNames: { '#eventType': 'eventType' },
                        ExpressionAttributeValues: {
                            ':socialEvent': [{
                                id: tempEvent.id,
                                eventURL: tempEvent.eventURL,
                                eventTitle: tempEvent.eventTitle,
                                location: tempEvent.location,
                                address: tempEvent.address,
                                city: tempEvent.city,
                                state: tempEvent.state,
                                zip: tempEvent.zip,
                                img: tempEvent.img,
                                date: tempEvent.date,
                            }],
                            ':emptyList': []
                        }
                    };
                    await docClient.send(new UpdateCommand(tempUpdateParams));
                    console.log(`Event added to existing date: ${tempDate}`);
                }else{
                    const putParams = {
                        TableName: tableName,
                        Item: {
                            date: tempDate,
                            eventType: {
                                celestial: [],
                                social: [{
                                    id: tempEvent.id,
                                    eventURL: tempEvent.eventURL,
                                    eventTitle: tempEvent.eventTitle,
                                    location: tempEvent.location,
                                    address: tempEvent.address,
                                    city: tempEvent.city,
                                    state: tempEvent.state,
                                    zip: tempEvent.zip,
                                    img: tempEvent.img,
                                    date: tempEvent.date,
                                }],
                            }
                        }
                    };
                    await docClient.send(new PutCommand(putParams));
                    console.log(`New date created with event: ${startDate}`);
                }
                //addEvent(tempDate, cleanTempEvent);
                //console.log("event that spans multiple days have been added");
            }
        }
        //console.log("function end");
        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Social events added successfully!' }),
        };
    } catch (error) {
        console.error('Error fetching or storing events:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: 'Error fetching or storing events' }),
        };
    }
};

function setDefaultValues(event) {
    return {
        ID: event.ID || 'N/A',
        eventURL: event.eventURL || 'N/A',
        eventTitle: event.eventTitle || 'Untitled Event',
        location: event.location || 'Unknown Location',
        address: event.address || 'Unknown Address',
        city: event.city || 'Unknown City',
        state: event.state || 'Unknown State',
        zip: event.zip || 'Unknown Zip',
        img: event.img || 'N/A'
    };
}