/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.scalar.timestamp;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

public class TimezoneFunction extends Scalar<Long, Object> {

    public static final String NAME = "timezone";
    private static final ZoneId UTC = ZoneId.of("UTC");
    private final DataType returnType;

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature(),
                DataTypes.TIMESTAMP.getTypeSignature()
            ),
            TimezoneFunction::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ),
            TimezoneFunction::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ),
            TimezoneFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private TimezoneFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.returnType = boundSignature.getReturnType().createType();
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args.length == 2 : String.format(Locale.ENGLISH,
                                                "number of arguments must be 2, got %d instead",
                                                args.length);
        String zoneStr = (String) args[0].value();
        if (zoneStr == null) {
            return null;
        }
        ZoneId zoneId;
        try {
            zoneId = ZoneId.of(zoneStr);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             " time zone \"%s\" not recognized",
                                                             zoneStr));
        }
        Number utcTimestamp = (Number) args[1].value();
        if (utcTimestamp == null) {
            return null;
        }

        Instant instant = Instant.ofEpochMilli(utcTimestamp.longValue());
        boolean paramHadTimezone = returnType == DataTypes.TIMESTAMP;
        ZoneId srcZoneId = paramHadTimezone ? zoneId : UTC;
        ZoneId dstZoneId = paramHadTimezone ? UTC : zoneId;
        ZonedDateTime zonedDateTime = instant.atZone(srcZoneId).withZoneSameLocal(dstZoneId);
        return zonedDateTime.toInstant().toEpochMilli();
    }
}
