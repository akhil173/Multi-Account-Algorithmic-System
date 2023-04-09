# Multi-Account-Algorithmic-System
An algorithmic trading system that operates for multiple accounts concurrently and executes trades using Angel One's Smart API.

### Required Libraries
```
smartapi
pandas
numpy
fyers_api
pyotp
```

### Logic
This is an Algorithmic Trading System for the typical Short Straddle Startegy that runs in the Stock Markets.

It is a premium based Short Straddle, so we use numpy to get the minimum argument in terms of Premiums.
A system that executes the trades concurrently for all the accounts.

The account details format is as follows:
• app_key	
• app_secret	
• username	
• password	
• mpin	
• dob	
• totp_key	
• lots	
• symbol	
• access_token	
• last_login

Here the access token is updated once a day and the last login stores the date when the access token was generated.

### Note
Google Sheets API is used to work with the data stored in Google sheets.
Google Service bot is used to access sheet with private data sharing.

### The Snapshot of how the Input Sheet should look.

![Screenshot 2023-04-09 23 22 15](https://user-images.githubusercontent.com/73926989/230788604-9fdafa8b-f460-4c1c-b43b-8dfee3d0c386.png)

