use assignment_2_solution::{
    deserialize_register_command, serialize_register_command, ClientCommandHeader,
    ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SystemCommandHeader,
    SystemRegisterCommand, SystemRegisterCommandContent, SectorVec
};
use assignment_2_test_utils::transfer::*;
use ntest::timeout;
use uuid::Uuid;

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity() {
    // given
    let request_identifier = 7;
    let sector_idx = 8;
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
                                    header,
                                    content: ClientRegisterCommandContent::Read,
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Read command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialized_read_proc_cmd_has_correct_format() {
    // given
    let sector_idx = 4525787855454_u64;
    let process_identifier = 147_u8;
    let msg_ident = [7; 16];

    let read_proc_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident: Uuid::from_slice(&msg_ident).unwrap(),
            sector_idx,
        },
        content: SystemRegisterCommandContent::ReadProc,
    });
    let mut serialized: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&read_proc_cmd, &mut serialized, &[0x00_u8; 64])
        .await
        .expect("Could not write to vector?");
    serialized.truncate(serialized.len() - 32);

    // then
    assert_eq!(serialized.len(), 32);
    assert_system_cmd_header(
        serialized.as_slice(),
        &msg_ident,
        process_identifier,
        3,
        sector_idx,
    );
}


// MIEI TEST
#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_write() {
    // given
    let request_identifier = 40;
    let sector_idx = 21212;

    let mut content: Vec<u8> = vec![7u8; 4096];
    content[0] = 0;
    content[1] = 1;
    content[2] = 2;
    content[3] = 3;

    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(content.clone())
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    println!("{:?}", slice);
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    let mut sink: Vec<u8> = Vec::new();

    assert!(hmac_valid);
    match &deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
                                    header,
                                    content: ClientRegisterCommandContent::Write {
                                        data: SectorVec(cmd_content)
                                    },
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
            assert_eq!(*cmd_content, content);
        }
        _ => panic!("Expected Write command"),
    }

    serialize_register_command(&deserialized_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    println!("{:?}", sink)
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_write_proc() {
    // given
    let request_identifier = Uuid::from_u128(69);
    let sector_idx = 21212;
    let process_identifier = 23;
    let timestamp = 29u64;
    let value_wr = 8;

    let mut content: Vec<u8> = vec![7u8; 4096];
    content[0] = 0;
    content[1] = 1;
    content[2] = 2;
    content[3] = 3;

    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            msg_ident: request_identifier,
            process_identifier,
            sector_idx,
        },
        content: SystemRegisterCommandContent::WriteProc {
            data_to_write: SectorVec(content.clone()),
            timestamp,
            write_rank: value_wr
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    let mut sink: Vec<u8> = Vec::new();

    assert!(hmac_valid);
    match &deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
                                    header,
                                    content: SystemRegisterCommandContent::WriteProc {
                                        data_to_write: SectorVec(cmd_content),
                                        write_rank: cmd_write_rank,
                                        timestamp: cmd_timestamp
                                    },
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(*cmd_content, content);
            assert_eq!(header.msg_ident, request_identifier);
            assert_eq!(*cmd_write_rank, value_wr);
            assert_eq!(*cmd_timestamp, timestamp);
        }
        _ => panic!("Expected Write Proc command"),
    }

    serialize_register_command(&deserialized_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_value() {
    // given
    let request_identifier = Uuid::from_u128(69);
    let sector_idx = 21212;
    let process_identifier = 23;
    let timestamp = 29u64;
    let value_wr = 8;

    let mut content: Vec<u8> = vec![7u8; 4096];
    content[0] = 0;
    content[1] = 1;
    content[2] = 2;
    content[3] = 3;

    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            msg_ident: request_identifier,
            process_identifier,
            sector_idx,
        },
        content: SystemRegisterCommandContent::Value {
            sector_data: SectorVec(content.clone()),
            timestamp,
            write_rank: value_wr
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    let mut sink: Vec<u8> = Vec::new();

    assert!(hmac_valid);
    match &deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
                                    header,
                                    content: SystemRegisterCommandContent::Value {
                                        sector_data: SectorVec(cmd_content),
                                        write_rank: cmd_write_rank,
                                        timestamp: cmd_timestamp
                                    },
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(*cmd_content, content);
            assert_eq!(header.msg_ident, request_identifier);
            assert_eq!(*cmd_write_rank, value_wr);
            assert_eq!(*cmd_timestamp, timestamp);
        }
        _ => panic!("Expected Value command"),
    }

    serialize_register_command(&deserialized_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_read_proc() {
    // given
    let request_identifier = Uuid::from_u128(69);
    let sector_idx = 21212;
    let process_identifier = 23;

    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            msg_ident: request_identifier,
            process_identifier,
            sector_idx,
        },
        content: SystemRegisterCommandContent::ReadProc
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    let mut sink: Vec<u8> = Vec::new();

    assert!(hmac_valid);
    match &deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
                                    header,
                                    content: SystemRegisterCommandContent::ReadProc
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(header.msg_ident, request_identifier);
        }
        _ => panic!("Expected Read Proc command"),
    }

    serialize_register_command(&deserialized_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_ack() {
    // given
    let request_identifier = Uuid::from_u128(69);
    let sector_idx = 21212;
    let process_identifier = 23;

    let mut content: Vec<u8> = vec![7u8; 4096];
    content[0] = 0;
    content[1] = 1;
    content[2] = 2;
    content[3] = 3;

    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            msg_ident: request_identifier,
            process_identifier,
            sector_idx,
        },
        content: SystemRegisterCommandContent::Ack
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    let mut sink: Vec<u8> = Vec::new();

    assert!(hmac_valid);
    match &deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
                                    header,
                                    content: SystemRegisterCommandContent::Ack
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(header.msg_ident, request_identifier);
        }
        _ => panic!("Expected Ack command"),
    }

    serialize_register_command(&deserialized_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_slides_until_magic_number() {
    // given
    let request_identifier = 7;
    let sector_idx = 8;
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    //-------------
    let binding = [&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], slice].concat();
    slice = binding.as_slice();
    //-------------

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
                                    header,
                                    content: ClientRegisterCommandContent::Read,
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Read command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_rejects_invalid_hmac() {
    // given
    let request_identifier = 7;
    let sector_idx = 8;
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let slice: &mut[u8] = &mut sink[..];

    slice[slice.len() - 1] = slice[slice.len() - 1].wrapping_add(1);
    let mut slice : &[u8] = slice;

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    assert!(!hmac_valid);
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
                                    header,
                                    content: ClientRegisterCommandContent::Read,
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Read command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_slides_over_header_if_msg_type_invalid() {
    // given
    let request_identifier = 7;
    let sector_idx = 8;
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    //------------- Header with invalid msg_type
    let binding = [&[0x61u8, 0x74, 0x64, 0x64, 0, 0, 0, 20], slice].concat();
    slice = binding.as_slice();
    //-------------

    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;

    // First deserialization should fail and slide over
    deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
        .await
        .expect_err("Could not deserialize");

    // Second deserialization should work
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
                                    header,
                                    content: ClientRegisterCommandContent::Read,
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Read command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_slides_over_whole_msg_if_hmac_invalid() {
    // given
    let request_identifier = Uuid::from_u128(69);
    let sector_idx = 21212;
    let process_identifier = 23;
    let timestamp = 29u64;
    let value_wr = 8;

    let mut content: Vec<u8> = vec![7u8; 4096];
    content[0] = 0;
    content[1] = 1;
    content[2] = 2;
    content[3] = 3;

    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            msg_ident: request_identifier,
            process_identifier,
            sector_idx,
        },
        content: SystemRegisterCommandContent::Value {
            sector_data: SectorVec(content.clone()),
            timestamp,
            write_rank: value_wr
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");

    // CHANGE HMAC OF FIRST MESSAGE
    let slice: &mut[u8] = &mut sink[..];

    slice[slice.len() - 1] = slice[slice.len() - 1].wrapping_add(1);

    // WRITE SECOND TIME
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];

    // READ FIRST TIME, SHOULD BE FALSE
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (_, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    assert!(!hmac_valid);

    // READ SECOND_TIME
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) =
        deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    assert!(hmac_valid);
    match &deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
                                    header,
                                    content: SystemRegisterCommandContent::Value {
                                        sector_data: SectorVec(cmd_content),
                                        write_rank: cmd_write_rank,
                                        timestamp: cmd_timestamp
                                    },
                                }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(*cmd_content, content);
            assert_eq!(header.msg_ident, request_identifier);
            assert_eq!(*cmd_write_rank, value_wr);
            assert_eq!(*cmd_timestamp, timestamp);
        }
        _ => panic!("Expected Value command"),
    }
}