FROM node:18-slim

COPY ./ /tardis-machine
WORKDIR /tardis-machine
RUN npm install
RUN npx tsc
# run it
CMD node ./bin/tardis-machine.js --debug=true --cache-dir=/.cache