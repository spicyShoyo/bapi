#!/bin/bash
# genereates test fixtures and protobuf
cd internal/store/fixtures/; python3 gen_fixture.py; cd -; sh pb.sh