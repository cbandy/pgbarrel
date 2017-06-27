package pgbarrel

import (
	"bytes"
	"regexp"
)

type pgTestDecoding struct{}

var (
	pgTestDecodingTypeRegexp = regexp.MustCompile(`^\[.+?\]:`)
)

func (p pgTestDecoding) Parse(input []byte, output *ReplicationOperation) error {
	output.OldColumns = output.OldColumns[:0]
	output.NewColumns = output.NewColumns[:0]
	output.OldValues = output.OldValues[:0]
	output.NewValues = output.NewValues[:0]

	if len(input) < 1 {
		panic(`FIXME`)
	}

	if input[0] == 'B' { // BEGIN
		output.Operation = string(input[:5])
		output.Target = string(input[6:])
		return nil
	}

	if input[0] == 'C' { // COMMIT
		output.Operation = string(input[:6])
		output.Target = string(input[7:])
		return nil
	}

	if !bytes.HasPrefix(input, []byte(`table `)) {
		panic(`FIXME`)
	}

	current, target := pgParseIdentifier(input[6:])

	if !bytes.HasPrefix(current, []byte(`: `)) {
		panic(`FIXME`)
	}

	current, operation := current[2+6:], current[2:2+6]

	if !bytes.Equal(operation, []byte(`DELETE`)) && !bytes.Equal(operation, []byte(`INSERT`)) && !bytes.Equal(operation, []byte(`UPDATE`)) {
		panic(`FIXME`)
	}

	if !bytes.HasPrefix(current, []byte(`: `)) {
		panic(`FIXME`)
	}

	current = current[2:]
	output.Target = string(target)
	output.Operation = string(operation)

	switch output.Operation[0] {
	case 'D': // DELETE
		return p.parseDelete(current, output)
	case 'I': // INSERT
		return p.parseInsert(current, output)
	case 'U': // UPDATE
		return p.parseUpdate(current, output)
	}

	// TODO error
	return nil
}

func (pgTestDecoding) parseColumn(src []byte) (remaining, name, value []byte, err error) {
	src, name = pgParseIdentifier(src)

	match := pgTestDecodingTypeRegexp.Find(src)
	if match == nil {
		panic(`FIXME`)
	}

	src, value = pgParseConstant(src[len(match):])

	if len(src) > 0 && src[0] == ' ' {
		src = src[1:]
	}

	return src, name, value, nil
}

func (p pgTestDecoding) parseDelete(input []byte, output *ReplicationOperation) error {
	var err error
	var name, value []byte

	for len(input) > 0 {
		if input, name, value, err = p.parseColumn(input); err != nil {
			return err
		}

		output.OldColumns = append(output.OldColumns, string(name))
		output.OldValues = append(output.OldValues, string(value))
	}

	return nil
}

func (p pgTestDecoding) parseInsert(input []byte, output *ReplicationOperation) error {
	var err error
	var name, value []byte

	for len(input) > 0 {
		if input, name, value, err = p.parseColumn(input); err != nil {
			return err
		}

		output.NewColumns = append(output.NewColumns, string(name))
		output.NewValues = append(output.NewValues, string(value))
	}

	return nil
}

func (p pgTestDecoding) parseUpdate(input []byte, output *ReplicationOperation) error {
	var err error
	var name, value []byte

	if bytes.HasPrefix(input, []byte(`old-key: `)) {
		input = input[9:]

		for len(input) > 0 {
			if input, name, value, err = p.parseColumn(input); err != nil {
				return err
			}

			output.OldColumns = append(output.OldColumns, string(name))
			output.OldValues = append(output.OldValues, string(value))

			if bytes.HasPrefix(input, []byte(`new-tuple: `)) {
				input = input[11:]
				break
			}
		}
	}

	for len(input) > 0 {
		if input, name, value, err = p.parseColumn(input); err != nil {
			return err
		}

		output.NewColumns = append(output.NewColumns, string(name))
		output.NewValues = append(output.NewValues, string(value))
	}

	return nil
}
